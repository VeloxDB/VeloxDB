using System;
using VeloxDB.ObjectInterface;
using VeloxDB.Protocol;
using VlxBlog.DTO;

namespace VlxBlog;

[DbAPI]
public class BlogApi
{
	[DbAPIOperation]
	public long CreateBlog(ObjectModel om, BlogDTO blog)
	{
		Blog newBlog = Blog.FromDTO(om, blog);
		return newBlog.Id;
	}

	[DbAPIOperation(OperationType = DbAPIOperationType.Read)]
	public BlogDTO? GetBlog(ObjectModel om, long id)
	{
		Blog? blog = om.GetObject<Blog>(id);

		if (blog == null)
			return null;

		foreach(var post in blog.Posts)
		{
			post.Select();
		}

		return blog.ToDTO();
	}

	[DbAPIOperation]
	public bool Update(ObjectModel om, BlogDTO update)
	{
		Blog? blog = om.GetObject<Blog>(update.Id);
		if (blog == null)
			return false;

		blog.Url = update.Url;
		return true;
	}

	[DbAPIOperation]
	public bool AddPost(ObjectModel om, PostDTO? post)
	{
		if (post == null || post.BlogId == 0)
			return false;

		Post.FromDTO(om, post);
		return true;
	}

	[DbAPIOperation]
	public bool DeletePost(ObjectModel om, long id)
	{
		Post? post = om.GetObject<Post>(id);
		if (post == null)
			return false;

		post.Delete();
		return true;
	}

	[DbAPIOperation]
	public bool DeleteBlog(ObjectModel om, long id)
	{
		Blog? blog = om.GetObject<Blog>(id);
		if (blog == null)
			return false;

		blog.Delete();
		return true;
	}
}
