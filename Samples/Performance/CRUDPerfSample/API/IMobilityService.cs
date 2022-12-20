using System;
using System.Xml.Linq;
using VeloxDB.Client;
using VeloxDB.Protocol;
namespace API;

[DbAPI(Name = "MobilityService")]
public interface IMobilityService
{
	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None)]
	DatabaseTask<long[]> InsertVehicles(VehicleDTO[] vehicleDTOs);

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None)]
	DatabaseTask UpdateVehiclePositions(long[] vehicleIds, double positionX, double positionY);

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None)]
	DatabaseTask CopyVehiclePositions(long[] srcVehicleIds, long[] dstVehicleIds);

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None)]
	DatabaseTask DeleteVehicles(long[] vehicleIds);

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None)]
	DatabaseTask<long[]> InsertRides(RideDTO[] rideDTOs);

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None)]
	DatabaseTask UpdateRides(RideDTO[] rideDTOs);

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None)]
	DatabaseTask DeleteRides(long[] rideIds);

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None, OperationType = DbAPIOperationType.Read)]
	DatabaseTask<VehicleDTO[]> GetVehicles(long[] vehicleIds);

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None, OperationType = DbAPIOperationType.Read)]
	DatabaseTask<VehicleDTO[]> GetRideVehicle(long[] rideIds);

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None, OperationType = DbAPIOperationType.Read)]
	DatabaseTask<RideDTO[][]> GetVehicleRides(long[] vehicleIds);

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None, OperationType = DbAPIOperationType.Read)]
	DatabaseTask<int> GetVehicleYear(long id);
}
