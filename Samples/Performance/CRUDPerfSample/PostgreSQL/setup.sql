CREATE TABLE Vehicles (
    Id BIGSERIAL PRIMARY KEY,
    PositionX DOUBLE PRECISION NOT NULL,
    PositionY DOUBLE PRECISION NOT NULL,
    ModelName VARCHAR(255) NOT NULL,
    Year INTEGER NOT NULL,
    PassengerCapacity INTEGER NOT NULL DEFAULT 5
);

CREATE TABLE Rides (
    Id BIGSERIAL PRIMARY KEY,
    VehicleId BIGINT NOT NULL REFERENCES Vehicles(Id) ON DELETE CASCADE,
    StartTime TIMESTAMPTZ NOT NULL,
    EndTime TIMESTAMPTZ NOT NULL,
    CoveredDistance DOUBLE PRECISION NOT NULL
);

CREATE INDEX idx_vehicleid_starttime ON Rides (VehicleId);

CREATE TYPE VehicleDTO AS (
    PositionX DOUBLE PRECISION,
    PositionY DOUBLE PRECISION,
    ModelName VARCHAR(255),
    Year INTEGER,
    PassengerCapacity INTEGER
);

CREATE TYPE RideDTO AS (
    Id BIGINT,
    VehicleId BIGINT,
    StartTime TIMESTAMPTZ,
    EndTime TIMESTAMPTZ,
    CoveredDistance DOUBLE PRECISION
);

CREATE TYPE SourceDestinationPair AS (
    SrcVehicleId BIGINT,
    DstVehicleId BIGINT
);

CREATE OR REPLACE FUNCTION InsertVehicles(p_vehicleDTOs VehicleDTO[])
RETURNS BIGINT[]
AS $$
DECLARE
    v_ids BIGINT[];
	v_id BIGINT;
    v_dto VehicleDTO;
BEGIN
    FOR i IN 1 .. array_length(p_vehicleDTOs, 1)
    LOOP
        v_dto := p_vehicleDTOs[i];
        INSERT INTO Vehicles (PositionX, PositionY, ModelName, Year, PassengerCapacity)
        VALUES (v_dto.PositionX, v_dto.PositionY, v_dto.ModelName, v_dto.Year, v_dto.PassengerCapacity)
        RETURNING Id INTO v_id;
		v_ids[i] = v_id;
    END LOOP;
    RETURN v_ids;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION DeleteVehicles(p_vehicleIds BIGINT[])
RETURNS VOID
AS $$
BEGIN
    DELETE FROM Vehicles WHERE Id = ANY(p_vehicleIds);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION UpdateVehiclePositions(p_vehicle_ids BIGINT[], p_position_x DOUBLE PRECISION, p_position_y DOUBLE PRECISION)
RETURNS VOID
AS $$
BEGIN
    UPDATE Vehicles
    SET PositionX = p_position_x, PositionY = p_position_y
    WHERE Id = ANY(p_vehicle_ids);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION CopyVehiclePositions(p_src_dst_pairs SourceDestinationPair[])
RETURNS VOID
AS $$
BEGIN
    WITH src_dst_pairs(p) AS (
        SELECT UNNEST(p_src_dst_pairs) AS pair
    ), src_vehicles AS (
        SELECT id, PositionX, PositionY, (pair.p).DstVehicleId AS dst_vehicle_id
        FROM Vehicles, src_dst_pairs pair
        WHERE Vehicles.id = (pair.p).SrcVehicleId
    )
    UPDATE Vehicles
    SET PositionX = src_vehicles.PositionX, PositionY = src_vehicles.PositionY
    FROM src_vehicles
    WHERE Vehicles.id = src_vehicles.dst_vehicle_id;

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION InsertRides(p_ride_dtos RideDTO[])
RETURNS BIGINT[]
AS $$
DECLARE
    v_ids BIGINT[];
	v_id BIGINT;
    v_dto RideDTO;
BEGIN
    FOR i IN 1 .. array_length(p_ride_dtos, 1)
    LOOP
        v_dto := p_ride_dtos[i];
        INSERT INTO Rides (VehicleId, StartTime, EndTime, CoveredDistance)
        VALUES (v_dto.VehicleId, v_dto.StartTime, v_dto.EndTime, v_dto.CoveredDistance)
        RETURNING Id INTO v_id;
		v_ids[i] = v_id;
    END LOOP;
    RETURN v_ids;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION UpdateRides(p_ride_dtos RideDTO[])
RETURNS VOID
AS $$
DECLARE
    v_dto RideDTO;
BEGIN
    FOR i IN 1 .. array_length(p_ride_dtos, 1)
    LOOP
        v_dto := p_ride_dtos[i];
        UPDATE Rides
        SET VehicleId = v_dto.VehicleId, StartTime = v_dto.StartTime, EndTime = v_dto.EndTime, CoveredDistance = v_dto.CoveredDistance
        WHERE Id = v_dto.Id;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION DeleteRides(p_ride_ids BIGINT[])
RETURNS VOID
AS $$
BEGIN
    DELETE FROM Rides WHERE Id = ANY(p_ride_ids);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION GetRideVehicles(p_ride_ids BIGINT[])
RETURNS TABLE(Id BIGINT, PositionX DOUBLE PRECISION, PositionY DOUBLE PRECISION, ModelName VARCHAR(255), Year INTEGER, PassengerCapacity INTEGER)
AS $$
BEGIN
    RETURN QUERY 
    SELECT v.Id, v.PositionX, v.PositionY, v.ModelName, v.Year, v.PassengerCapacity
    FROM Vehicles v
    JOIN Rides r ON v.Id = r.VehicleId
    WHERE r.Id = ANY(p_ride_ids);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION GetVehicleRides(p_vehicle_ids BIGINT[])
RETURNS TABLE(VehicleId BIGINT, RideId BIGINT, StartTime TIMESTAMPTZ, EndTime TIMESTAMPTZ, CoveredDistance DOUBLE PRECISION)
AS $$
BEGIN
    RETURN QUERY 
    SELECT r.VehicleId, r.Id, r.StartTime, r.EndTime, r.CoveredDistance
    FROM Rides r
    WHERE r.VehicleId = ANY(p_vehicle_ids)
    ORDER BY r.VehicleId;
END;
$$ LANGUAGE plpgsql;
