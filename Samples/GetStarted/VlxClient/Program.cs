using VlxClient;
using Velox.Client;
using VlxBlog.DTO;

ConnectionStringParams csp = new ConnectionStringParams();
csp.AddAddress("localhost:7568");

IBlogAPI blogApi = ConnectionFactory.Get<IBlogAPI>(csp.GenerateConnectionString());

// Create
long id = blogApi.CreateBlog(new BlogDTO { Url = "http://example.com/blog" });

// Get
var blog = blogApi.GetBlog(id);
if (blog == null)
{
	Console.WriteLine("Couldn't find blog");
	return;
}
else
{
	Console.WriteLine("Blog retrieved from server");
}

// Add post
bool result = blogApi.AddPost(new PostDTO { BlogId = blog.Id, Title = "Hello world", Content = "My first Velox App" });
Console.WriteLine($"Adding post success: {result}");

// Delete
result = blogApi.DeleteBlog(id);
Console.WriteLine($"Deleting blog success: {result}");



