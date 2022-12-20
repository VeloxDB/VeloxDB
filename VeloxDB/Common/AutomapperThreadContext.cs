using System;
using System.Collections.Generic;
using System.ComponentModel;

namespace VeloxDB.Common;

#pragma warning disable 1591
[EditorBrowsableAttribute(EditorBrowsableState.Never)]
public class AutomapperThreadContext
{
	FastDictionary<object, object> map { get; set; }
	Queue<Action<AutomapperThreadContext>> queue {get; set;}

	public AutomapperThreadContext()
	{
		map = new FastDictionary<object, object>(512, ReferenceEqualityComparer<object>.Instance);
		queue = new Queue<Action<AutomapperThreadContext>>(16);
	}

	public bool AllowUpdate { get; set; }

	[ThreadStatic]
	private static AutomapperThreadContext instance;

	public static AutomapperThreadContext Instance
	{
		get
		{
			AutomapperThreadContext temp = instance;
			if(temp != null)
				return temp;

			temp = new AutomapperThreadContext();
			instance = temp;
			return temp;
		}
	}

	public void Enqueue(Action<AutomapperThreadContext> resume)
	{
		queue.Enqueue(resume);
	}

	public bool Dequeue(out Action<AutomapperThreadContext> resume)
	{
		return queue.TryDequeue(out resume);
	}

	public bool TryGet(object key, out object value)
	{
		return map.TryGetValue(key, out value);
	}

	public void Add(object key, object value)
	{
		map.Add(key, value);
	}

	public void Clear()
	{
		AllowUpdate = false;
		map.Clear();
		queue.Clear();
	}
}
#pragma warning restore 1591
