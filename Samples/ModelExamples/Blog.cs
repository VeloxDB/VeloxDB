using Velox.Descriptor;
using Velox.ObjectInterface;
using Velox.Protocol;

namespace VlxBlog;

#region Blog
[DatabaseClass]
public abstract class Blog : DatabaseObject
{
	[DatabaseProperty]
	public abstract string Url { get; set; }

	[InverseReferences(nameof(Post.Blog))]
	public abstract InverseReferenceSet<Post> Posts { get; }
}
#endregion

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

[DbAPI]
public class BlogService
{
	#region TestBlog
	[DbAPIOperation]
	public bool TestBlog(ObjectModel om)
	{
		bool result = true;
		// Create new blog.
		Blog blog = om.CreateObject<Blog>();

		// Create a new post.
		Post post1 = om.CreateObject<Post>();

		// Add a new post using direct reference.
		post1.Blog = blog;

		// Create another post.
		Post post2 = om.CreateObject<Post>();

		// Add another post to the blog, using inverse reference.
		blog.Posts.Add(post2);

		// Check if both posts are in blog
		result &= blog.Posts.Contains(post1);
		result &= blog.Posts.Contains(post2);

		// Check if both posts reference blog
		result &= post1.Blog == blog;
		result &= post2.Blog == blog;

		// Clear all posts
		blog.Posts.Clear();

		// Confirm that posts are not in blog anymore.
		result &= !blog.Posts.Contains(post1);
		result &= !blog.Posts.Contains(post2);

		// Check if both posts point to null.
		result &= post1.Blog == null;
		result &= post2.Blog == null;

		// Delete posts.
		post1.Delete();
		post2.Delete();

		// Delete blog.
		blog.Delete();

		return result;
	}
	#endregion
}
