using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Velox.Common;

namespace Velox.Descriptor;

internal sealed class XMLModelSettings : ModelSettings, IDisposable
{
	bool ownsStreams;
	List<Stream> xmlStreams;
	bool usedUp;
	bool disposed;

	public XMLModelSettings(bool ownsStreams = true)
	{
		this.ownsStreams = ownsStreams;
		xmlStreams = new List<Stream>();
	}

	~XMLModelSettings()
	{
		CleanUp(false);
	}

	public void Dispose()
	{
		CleanUp(true);
		GC.SuppressFinalize(this);
	}

	public void AddStream(Stream xmlStream)
	{
		Checker.NotNull(xmlStream, nameof(xmlStream));

		if (usedUp)
			throw new InvalidOperationException("Additional streams are not allowed. Meta model has already been instantiated.");

		if (disposed)
			throw new ObjectDisposedException(nameof(XMLModelSettings));

		xmlStreams.Add(xmlStream);
	}

	public override DataModelDescriptor CreateModel(PersistenceSettings persistenceSettings, DataModelDescriptor previousModel)
	{
		if (usedUp)
			throw new InvalidOperationException("Additional streams are not allowed. Meta model has already been instantiated.");

		if (disposed)
			throw new ObjectDisposedException(nameof(XMLModelSettings));

		DataModelDescriptor model = new DataModelDescriptor();
		for (int i = 0; i < xmlStreams.Count; i++)
		{
			model.Register(xmlStreams[i]);
		}

		model.Prepare(persistenceSettings);

		usedUp = true;
		Dispose();

		return model;
	}

	public override DataModelDescriptor CreateModel(DataModelDescriptor previousModel)
	{
		return CreateModel(null, previousModel);
	}

	private void CleanUp(bool isDisposing)
	{
		if (!disposed)
		{
			if (isDisposing && ownsStreams)
			{
				for (int i = 0; i < xmlStreams.Count; i++)
				{
					xmlStreams[i].Dispose();
				}
			}
		}

		disposed = true;
	}
}
