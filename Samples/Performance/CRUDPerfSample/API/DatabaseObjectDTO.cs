using System;

namespace API;

public abstract class DatabaseObjectDTO
{
	public long Id { get; set; }

	public DatabaseObjectDTO(long id)
	{
		this.Id = id;
	}

	public DatabaseObjectDTO()
	{
	}
}
