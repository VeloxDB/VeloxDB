using API;
using VeloxDB.Descriptor;
using VeloxDB.ObjectInterface;

namespace Server;

#region Ride
[DatabaseClass]
public abstract partial class Ride : DatabaseObject
{
	[DatabaseReference(isNullable: false, deleteTargetAction: DeleteTargetAction.CascadeDelete)]
	public abstract Vehicle Vehicle { get; set; }

	[DatabaseProperty]
	public abstract DateTime StartTime { get; set; }

	[DatabaseProperty]
	public abstract DateTime EndTime { get; set; }

	[DatabaseProperty]
	public abstract double CoveredDistance { get; set; }

	public Ride()
	{
	}
}
#endregion Ride

partial class Ride
{
	public void FromDTO(ObjectModel om, RideDTO dto)
	{
		Vehicle = om.GetObject<Vehicle>(dto.VehicleId)!;
		StartTime = dto.StartTime;
		EndTime = dto.EndTime;
		CoveredDistance = dto.CoveredDistance;
	}

	public RideDTO ToDTO()
	{
		return new RideDTO(Vehicle.Id, StartTime, EndTime, CoveredDistance);
	}
}
