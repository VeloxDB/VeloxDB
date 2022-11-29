using System;

namespace API;

public sealed class VehicleDTO : DatabaseObjectDTO
{
	public double PositionX { get; set; }
	public double PositionY { get; set; }
	public string ModelName { get; set; }
	public int Year { get; set; }
	public int PassengerCapacity { get; set; }

	public VehicleDTO()
	{
		ModelName = null!;
	}

	public VehicleDTO(double positionX, double positionY, string modelName, int year, int passengerCapacity) :
		this(0, positionX, positionY, modelName, year, passengerCapacity)
	{
	}

	public VehicleDTO(long id, double positionX, double positionY, string modelName, int year, int passengerCapacity) :
		base(id)
	{
		this.PositionX = positionX;
		this.PositionY = positionY;
		this.ModelName = modelName;
		this.Year = year;
		this.PassengerCapacity = passengerCapacity;
	}
}
