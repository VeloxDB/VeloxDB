using System;
using API;
using VeloxDB.ObjectInterface;

namespace Server;

#region Vehicle
[DatabaseClass]
public abstract partial class Vehicle : DatabaseObject
{
	[DatabaseProperty]
	public abstract double PositionX { get; set; }

	[DatabaseProperty]
	public abstract double PositionY { get; set; }

	[DatabaseProperty]
	public abstract string ModelName { get; set; }

	[DatabaseProperty]
	public abstract int Year { get; set; }

	[DatabaseProperty("5")]
	public abstract int PassengerCapacity { get; set; }

	[InverseReferences(nameof(Ride.Vehicle))]
	public abstract InverseReferenceSet<Ride> Rides { get; }

	public Vehicle()
	{
	}
}
#endregion Vehicle

partial class Vehicle
{
	public void FromDTO(VehicleDTO dto)
	{
		PositionX = dto.PositionX;
		PositionY = dto.PositionY;
		ModelName = dto.ModelName;
		Year = dto.Year;
		PassengerCapacity = dto.PassengerCapacity;
	}

	public VehicleDTO ToDTO()
	{
		return new VehicleDTO(PositionX, PositionY, ModelName, Year, PassengerCapacity);
	}
}
