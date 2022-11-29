using Velox.ObjectInterface;
using Velox.Protocol;

namespace Weather;

#region WeatherStation
[DatabaseClass]
public abstract class WeatherStation : DatabaseObject
{
	[DatabaseProperty]
	public abstract double Lat { get; set; }

	[DatabaseProperty]
	public abstract double Long { get; set; }

	[DatabaseProperty]
	public abstract DatabaseArray<double> Temperature { get; set; }

	[DatabaseProperty]
	public abstract DatabaseArray<DateTime> Dates { get; set; }
}
#endregion


#region City

[DatabaseClass]
[HashIndex("name", true, nameof(City.Name))]
public abstract class City : DatabaseObject
{
	[DatabaseProperty]
	public abstract string Name { get; set; }

	[DatabaseReference]
	public abstract ReferenceArray<WeatherStation> Stations {get; set;}
}
#endregion

#region WeatherService
[DbAPI]
public class WeatherService
{
	#region CreateTestStation
	[DbAPIOperation]
	public void CreateTestStation(ObjectModel om)
	{
		WeatherStation ws = om.CreateObject<WeatherStation>();

		// Create an empty array
		ws.Dates = DatabaseArray<DateTime>.Create(4);

		// Add new dates to the end
		ws.Dates.Add(new DateTime(2022, 7, 1));
		ws.Dates.Add(new DateTime(2022, 7, 2));
		ws.Dates.Add(new DateTime(2022, 7, 3));
		ws.Dates.Add(new DateTime(2022, 7, 4));

		// Create an array from an existing collection
		ws.Temperature = DatabaseArray<double>.Create(new double[] { 33, 39, 41, 34 });

		// Remove by value
		ws.Temperature.Remove(41);

		// Remove using index
		ws.Temperature.RemoveAt(2);

		// Clear
		ws.Temperature.Clear();
		ws.Dates.Clear();
	}
	#endregion

	#region CreateTestCity
	[DbAPIOperation]
	public void CreateTestCity(ObjectModel om)
	{
		City city = om.CreateObject<City>();

		city.Stations = new ReferenceArray<WeatherStation>();

		WeatherStation ws;
		for (int i = 0; i < 4; i++)
		{
			ws = om.CreateObject<WeatherStation>();
			city.Stations.Add(ws);
		}

		// Get a WeatherStation by index
		ws = city.Stations[2];

		// Remove using object
		city.Stations.Remove(ws);

		// Remove using index
		city.Stations.RemoveAt(0);

		// Clear
		city.Stations.Clear();
	}
	#endregion

	[DbAPIOperation]
	public int CountNewCities(ObjectModel om)
	{
		#region CountNewCities
		int count = 0;
		// Get cities enumerable.
		IEnumerable<City> cities = om.GetAllObjects<City>();

		// Iterate over cities
		foreach(City city in cities)
		{
			// Do your business logic with city.
			if(city.Name.Contains("New"))
			{
				count++;
			}

			// Abandon the object, since we wont be changing it.
			city.Abandon();
		}

		return count;
		#endregion CountNewCities
	}

	[DbAPIOperation]
	public double GetCityTempByName(ObjectModel om, string name)
	{
		#region GetCityTempByName
		HashIndexReader<City, string> reader = om.GetHashIndex<City, string>(nameof(City.Name));

		City city = reader.GetObject(name);

		if(city == null)
		{
			// City with given name not found.
			return double.NaN;
		}

		return city.Stations[0].Temperature.Last();
		#endregion GetCityTempByName
	}

}
#endregion
