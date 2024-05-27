using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using VeloxDB.Common;

namespace VeloxDB.Networking;

internal unsafe sealed class GroupingSender
{
	const int initSendBufferCapacity = 4096;// 1024 * 32;
	const int waitGroupSize = 64;   // Max WaitHandle.WaitAny count

	bool closed;
	Stream stream;

	SendBuffer[] sendBuffers1;
	SendBuffer[] sendBuffers2;

	object perCPUDataHandle;
	PerCPUData* perCPUData;

	ManualResetEvent[] dataReady = Enumerable.Range(0, ProcessorNumber.CoreCount).Select(x => new ManualResetEvent(false)).ToArray();

	CPUGroupWaiter[] waiters;

	public GroupingSender(Stream stream, bool isPriority)
	{
		this.stream = stream;

		sendBuffers1 = new SendBuffer[ProcessorNumber.CoreCount];
		sendBuffers2 = new SendBuffer[ProcessorNumber.CoreCount];
		for (int i = 0; i < sendBuffers1.Length; i++)
		{
			sendBuffers1[i] = new SendBuffer();
			sendBuffers2[i] = new SendBuffer();
		}

		perCPUData = (PerCPUData*)CacheLineMemoryManager.Allocate(sizeof(PerCPUData), out perCPUDataHandle);
		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			PerCPUData* p = (PerCPUData*)CacheLineMemoryManager.GetBuffer(perCPUData, i);
			PerCPUData.Init(p);
			p->sendBuffer1 = sendBuffers1[i].Pbuffer;
			p->sendBuffer2 = sendBuffers2[i].Pbuffer;
		}

		int groupCount = (dataReady.Length / waitGroupSize) + (dataReady.Length % waitGroupSize != 0 ? 1 : 0);
		waiters = new CPUGroupWaiter[groupCount];
		int c = 0;
		for (int i = 0; i < groupCount; i++)
		{
			int t = Math.Min(waitGroupSize, ProcessorNumber.CoreCount - c);
			waiters[i] = new CPUGroupWaiter(dataReady, c, Send, isPriority);
			c += t;
		}
	}

	~GroupingSender()
	{
		CacheLineMemoryManager.Free(perCPUDataHandle);
	}

	public bool Closed => closed;

	public bool Send(IntPtr buffer, int size)
	{
		int procNum = ProcessorNumber.GetCore();

		PerCPUData* pd = (PerCPUData*)CacheLineMemoryManager.GetBuffer(perCPUData, procNum);

		pd->sync.EnterWriteLock();
		try
		{
			if (closed)
				return false;

			SendBuffer sendBuffer = sendBuffers1[procNum];
			if (pd->size + size > sendBuffer.Buffer.Length)
				sendBuffer.Resize(pd);

			Utils.CopyMemory((byte*)buffer, (byte*)pd->sendBuffer1 + pd->size, size);
			pd->size += size;
			if (pd->size == size)
				dataReady[procNum].Set();

			return true;
		}
		finally
		{
			pd->sync.ExitWriteLock();
		}
	}

	private unsafe bool Send(CPUGroupWaiter waiter)
	{
		if (closed)
			return false;

		List<ArraySegment<byte>> sendSegments = waiter.SendSegments;

		for (int i = 0; i < waiter.CPUCount; i++)
		{
			int index = waiter.BaseCPU + i;
			PerCPUData* pd = (PerCPUData*)CacheLineMemoryManager.GetBuffer(perCPUData, index);

			pd->sync.EnterWriteLock();
			try
			{
				if (closed)
					return false;

				SendBuffer sendBuffer = sendBuffers1[index];

				if (pd->size > 0)
				{
					sendSegments.Add(new ArraySegment<byte>(sendBuffer.Buffer, 0, pd->size));
					pd->size = 0;
					ExchangeBuffers(index, pd);
					dataReady[index].Reset();
				}
			}
			finally
			{
				pd->sync.ExitWriteLock();
			}
		}

		Checker.AssertTrue(sendSegments.Count > 0);

		int n;
		try
		{
			lock (stream)
			{
				n = 0;
				for (int i = 0; i < sendSegments.Count; i++)
				{
					stream.Write(sendSegments[i]);
					n += sendSegments[i].Count;
				}
			}
		}
		catch (IOException)
		{
			n = 0;
		}

		sendSegments.Clear();

		if (n == 0)
		{
			Task.Run(() => Close());
			return false;
		}

		return true;
	}

	private void ExchangeBuffers(int index, PerCPUData* pd)
	{
		SendBuffer tb = sendBuffers1[index];
		sendBuffers1[index] = sendBuffers2[index];
		sendBuffers2[index] = tb;

		IntPtr tp = pd->sendBuffer1;
		pd->sendBuffer1 = pd->sendBuffer2;
		pd->sendBuffer2 = tp;
	}

	public void Close()
	{
		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			PerCPUData* pd = (PerCPUData*)CacheLineMemoryManager.GetBuffer(perCPUData, i);
			pd->sync.EnterWriteLock();
		}

		try
		{
			if (closed)
				return;

			closed = true;
			for (int i = 0; i < waiters.Length; i++)
			{
				waiters[i].SignalToTerminate();
			}
		}
		finally
		{
			for (int i = 0; i < ProcessorNumber.CoreCount; i++)
			{
				PerCPUData* pd = (PerCPUData*)CacheLineMemoryManager.GetBuffer(perCPUData, i);
				pd->sync.ExitWriteLock();
			}
		}

		for (int i = 0; i < waiters.Length; i++)
		{
			waiters[i].Terminate();
		}

		for (int i = 0; i < sendBuffers1.Length; i++)
		{
			sendBuffers1[i].Dispose();
			sendBuffers2[i].Dispose();
			dataReady[i].Close();
		}
	}

	private class SendBuffer
	{
		int size;
		byte[] buffer;
		GCHandle gcHandle;
		IntPtr pbuffer;

		public SendBuffer()
		{
			size = initSendBufferCapacity;
			buffer = GC.AllocateArray<byte>(initSendBufferCapacity, true);
			gcHandle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
			pbuffer = gcHandle.AddrOfPinnedObject();
		}

		public byte[] Buffer => buffer;
		public IntPtr Pbuffer => pbuffer;

		public void Dispose()
		{
			buffer = null;
			pbuffer = IntPtr.Zero;
			gcHandle.Free();
		}

		public void Resize(PerCPUData* pd)
		{
			byte[] newBuffer = GC.AllocateArray<byte>(buffer.Length * 2, true);
			Array.Copy(buffer, newBuffer, pd->size);
			gcHandle.Free();

			buffer = newBuffer;
			size = newBuffer.Length;
			gcHandle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
			pbuffer = gcHandle.AddrOfPinnedObject();
			pd->sendBuffer1 = pbuffer;
		}
	}

	private sealed class CPUGroupWaiter
	{
		int baseCPU;
		Thread worker;
		WaitHandle[] dataReady;
		List<ArraySegment<byte>> sendSegments;
		Func<CPUGroupWaiter, bool> callback;
		bool closed;

		public CPUGroupWaiter(ManualResetEvent[] dataReady, int baseCPU, Func<CPUGroupWaiter, bool> callback, bool isPriority)
		{
			this.baseCPU = baseCPU;
			this.dataReady = new WaitHandle[Math.Min(waitGroupSize, dataReady.Length - baseCPU)];
			this.sendSegments = new List<ArraySegment<byte>>(this.dataReady.Length);
			this.callback = callback;

			for (int i = 0; i < this.dataReady.Length; i++)
			{
				this.dataReady[i] = dataReady[baseCPU + i];
			}

			worker = new Thread(Worker);
			worker.IsBackground = true;
			if (isPriority)
				worker.Priority = ThreadPriority.AboveNormal;

			worker.Start();
		}

		public List<ArraySegment<byte>> SendSegments => sendSegments;
		public int BaseCPU => baseCPU;
		public int CPUCount => dataReady.Length;

		public void Worker()
		{
			while (true)
			{
				WaitHandle.WaitAny(dataReady);
				if (closed)
					return;

				if (!callback(this))
					return;
			}
		}

		public void SignalToTerminate()
		{
			closed = true;
			((ManualResetEvent)dataReady[0]).Set();
		}

		public void Terminate()
		{
			worker.Join();
		}
	}

	private struct PerCPUData
	{
		public RWLock sync;
		public IntPtr sendBuffer1;
		public IntPtr sendBuffer2;
		public int size;

		public static void Init(PerCPUData* d)
		{
			d->sync = new RWLock();
			d->size = 0;
		}
	}
}
