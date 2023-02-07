using System;

namespace VeloxDB.AspNet
{
    /// <summary>
    /// Attribute to indicate that a controller should not be generated for a given DbApi.
    /// </summary>
    [AttributeUsage(AttributeTargets.Interface, AllowMultiple = false)]
    public class DoNotGenerateControllerAttribute : Attribute
    {
    }
}