using System;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.Replication;

internal class ReplicaData
{
	const int defChangesetsCap = 16;

	TransactionContext owner;
	int index;

	int changesetCount;
	Changeset[] changesets;

	ReplicaDescriptor replicaDesc;

	public ReplicaData(TransactionContext owner, ReplicaDescriptor replicaDesc, int index)
	{
		this.owner = owner;
		this.index = index;
		this.replicaDesc = replicaDesc;
		changesets = new Changeset[defChangesetsCap];
	}

	public void AddChangeset(int sourceIndex, Changeset ch)
	{
		if (changesetCount == changesets.Length)
			Array.Resize(ref changesets, changesets.Length * 2);

		if (index == sourceIndex)
			return;

		ch.TakeRef();
		changesets[changesetCount++] = ch;
	}

	public void Merge(ReplicaData r)
	{
		Checker.AssertTrue(object.ReferenceEquals(replicaDesc, r.replicaDesc) && index == r.index);

		for (int i = 0; i < r.changesetCount; i++)
		{
			if (changesetCount == changesets.Length)
				Array.Resize(ref changesets, changesets.Length * 2);

			changesets[changesetCount++] = r.changesets[i];
		}

		r.changesetCount = 0;
	}

	public Changeset[] TakeChangesets()
	{
		Changeset[] chs = new Changeset[changesetCount];

		int c = 0;
		for (int i = 0; i < changesetCount; i++)
		{
			chs[c++] = changesets[i];
			changesets[i] = null;
		}

		changesetCount = 0;
		return chs;
	}

	public void Clear()
	{
		for (int i = 0; i < changesetCount; i++)
		{
			changesets[i].ReleaseRef();
			changesets[i] = null;
		}

		changesetCount = 0;

		if (changesets.Length > defChangesetsCap)
			changesets = new Changeset[defChangesetsCap];
	}
}
