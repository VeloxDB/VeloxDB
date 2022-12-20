using System;
using VeloxDB.Descriptor;
using VeloxDB.ObjectInterface;
using VlxBlog.DTO;

namespace VlxBlog;

[DatabaseClass]
public abstract class Blog : DatabaseObject
{
	[DatabaseProperty]
	public abstract string Url { get; set; }

	[InverseReferences(nameof(Post.Blog))]
	public abstract InverseReferenceSet<Post> Posts { get; }
}

#region Post
[DatabaseClass]
public abstract class Post : DatabaseObject
{
	[DatabaseProperty]
	public abstract string Title { get; set; }

	[DatabaseProperty]
	public abstract string Content { get; set; }

	[DatabaseReference(false, DeleteTargetAction.CascadeDelete, true)]
	public abstract Blog Blog { get; set; }
}
#endregion
