using System;
using Velox.Protocol;
using VlxBlog.DTO;

namespace VlxClient;

[DbAPI(Name = "VlxBlog.BlogApi")]
public interface IBlogAPI
{
	[DbAPIOperation]
	long CreateBlog(BlogDTO blog);

	[DbAPIOperation(OperationType = DbAPIOperationType.Read)]
	BlogDTO? GetBlog(long id);

	[DbAPIOperation]
	bool Update(BlogDTO update);

	[DbAPIOperation]
	bool AddPost(PostDTO post);

	[DbAPIOperation]
	bool DeletePost(long id);

	[DbAPIOperation]
	bool DeleteBlog(long id);
}
