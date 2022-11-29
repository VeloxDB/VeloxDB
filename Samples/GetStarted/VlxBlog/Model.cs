using System;
using Velox.Descriptor;
using Velox.ObjectInterface;
using VlxBlog.DTO;

namespace VlxBlog;

[DatabaseClass]
public abstract partial class Blog : DatabaseObject
{
	[DatabaseProperty]
	public abstract string Url { get; set; }

	[InverseReferences(nameof(Post.Blog))]
	public abstract InverseReferenceSet<Post> Posts { get; }

	public partial BlogDTO ToDTO();
	public static partial Blog FromDTO(ObjectModel om, BlogDTO blog);
}

#region Post
[DatabaseClass]
public abstract partial class Post : DatabaseObject
{
	[DatabaseProperty]
	public abstract string Title { get; set; }

	[DatabaseProperty]
	public abstract string Content { get; set; }

	[DatabaseReference(false, DeleteTargetAction.CascadeDelete, true)]
	public abstract Blog Blog { get; set; }

	public partial PostDTO ToDTO();
	public static partial Post FromDTO(ObjectModel om, PostDTO post);
}
#endregion
