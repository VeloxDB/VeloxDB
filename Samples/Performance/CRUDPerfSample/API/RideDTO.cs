using System;

namespace API;

public sealed class RideDTO : DatabaseObjectDTO
{
	public long VehicleId { get; set; }
	public DateTime StartTime { get; set; }
	public DateTime EndTime { get; set; }
	public double CoveredDistance { get; set; }

	public RideDTO()
	{
	}

	public RideDTO(long vehicleId, DateTime startTime, DateTime endTime, double coveredDistance) :
		this(0, vehicleId, startTime, endTime, coveredDistance)
	{
	}

	public RideDTO(long id, long vehicleId, DateTime startTime, DateTime endTime, double coveredDistance)
	{
		this.Id = id;
		this.VehicleId = vehicleId;
		this.StartTime = startTime;
		this.EndTime = endTime;
		this.CoveredDistance = coveredDistance;
	}
}
