using System;
using System.Diagnostics;
using API;
using Velox.ObjectInterface;
using Velox.Protocol;

namespace Server;

[DbAPI(Name = "MobilityService")]
public sealed class MobilityService
{
	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None)]
	public long[] InsertVehicles(ObjectModel om, VehicleDTO[] vehicleDTOs)
	{
		long[] ids = new long[vehicleDTOs.Length];
		for (int i = 0; i < vehicleDTOs.Length; i++)
		{
			Vehicle v = om.CreateObject<Vehicle>();
			v.FromDTO(vehicleDTOs[i]);
			ids[i] = v.Id;
		}

		return ids;
	}

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None)]
	public void UpdateVehiclePositions(ObjectModel om, long[] vehicleIds, double positionX, double positionY)
	{
		for (int i = 0; i < vehicleIds.Length; i++)
		{
			Vehicle v = om.GetObject<Vehicle>(vehicleIds[i]);
			v.PositionX = positionX;
			v.PositionY = positionY;
		}
	}

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None)]
	public void CopyVehiclePositions(ObjectModel om, long[] srcVehicleIds, long[] dstVehicleIds)
	{
		for (int i = 0; i < srcVehicleIds.Length; i++)
		{
			Vehicle src = om.GetObject<Vehicle>(srcVehicleIds[i]);
			Vehicle dst = om.GetObject<Vehicle>(dstVehicleIds[i]);
			dst.PositionX = src.PositionX;
			dst.PositionY = src.PositionY;
		}
	}

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None)]
	public void DeleteVehicles(ObjectModel om, long[] vehicleIds)
	{
		for (int i = 0; i < vehicleIds.Length; i++)
		{
			om.GetObject<Vehicle>(vehicleIds[i]).Delete();
		}
	}

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None)]
	public long[] InsertRides(ObjectModel om, RideDTO[] rideDTOs)
	{
		long[] ids = new long[rideDTOs.Length];
		for (int i = 0; i < rideDTOs.Length; i++)
		{
			Ride r = om.CreateObject<Ride>();
			r.FromDTO(om, rideDTOs[i]);
			ids[i] = r.Id;
		}

		return ids;
	}

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None)]
	public void UpdateRides(ObjectModel om, RideDTO[] rideDTOs)
	{
		for (int i = 0; i < rideDTOs.Length; i++)
		{
			Ride r = om.GetObject<Ride>(rideDTOs[i].Id);
			r.FromDTO(om, rideDTOs[i]);
		}
	}

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None)]
	public void DeleteRides(ObjectModel om, long[] rideIds)
	{
		for (int i = 0; i < rideIds.Length; i++)
		{
			om.GetObject<Ride>(rideIds[i]).Delete();
		}
	}

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None, OperationType = DbAPIOperationType.Read)]
	public VehicleDTO[] GetVehicles(ObjectModel om, long[] vehicleIds)
	{
		VehicleDTO[] vehiclesDTOs = new VehicleDTO[vehicleIds.Length];
		for (int i = 0; i < vehicleIds.Length; i++)
		{
			vehiclesDTOs[i] = om.GetObject<Vehicle>(vehicleIds[i]).ToDTO();
		}

		return vehiclesDTOs;
	}

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None, OperationType = DbAPIOperationType.Read)]
	public VehicleDTO[] GetRideVehicle(ObjectModel om, long[] rideIds)
	{
		VehicleDTO[] vehiclesDTOs = new VehicleDTO[rideIds.Length];
		for (int i = 0; i < rideIds.Length; i++)
		{
			Ride ride = om.GetObject<Ride>(rideIds[i]);
			vehiclesDTOs[i] = ride.Vehicle.ToDTO();
		}

		return vehiclesDTOs;
	}

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None, OperationType = DbAPIOperationType.Read)]
	public RideDTO[][] GetVehicleRides(ObjectModel om, long[] vehicleIds)
	{
		RideDTO[][] rideDTOs = new RideDTO[vehicleIds.Length][];
		for (int i = 0; i < vehicleIds.Length; i++)
		{
			Vehicle vehicle = om.GetObject<Vehicle>(vehicleIds[i]);
			rideDTOs[i] = vehicle.Rides.Select(x => x.ToDTO()).ToArray();
		}

		return rideDTOs;
	}
}
