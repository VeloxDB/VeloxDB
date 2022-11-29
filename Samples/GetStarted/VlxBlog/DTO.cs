using System;

namespace VlxBlog.DTO;

public class BlogDTO
{
	public BlogDTO()
	{
		Url = null!;
		Posts = null!;
	}

	public long Id { get; set; }
	public string Url { get; set; }
	public PostDTO[]? Posts { get; set; }
}

public class PostDTO
{
	public PostDTO()
	{
		Title = null!;
		Content = null!;
	}

	public string Title { get; set; }
	public string Content { get; set; }
	public long BlogId { get; set; }
}
