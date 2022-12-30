using System;

namespace VeloxDB.ClientApp;

internal abstract class Mode
{
	public abstract string Title { get; }
	public abstract Mode Parent { get; }

	public virtual bool Confirmation()
	{
		return true;
	}
}
