using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon;

namespace AWSHelpers
{
    public class AWSGeneralHelper
    {
        public static RegionEndpoint GetRegionEndpoint (string RegionName = "USEast1")
        {
            RegionEndpoint result = null;

            RegionName = RegionName.Replace ("-", "").ToLowerInvariant ();

            switch (RegionName)
            {
                case "apnortheast1":    { result = RegionEndpoint.APNortheast1;    break; }
                case "apsoutheast1":    { result = RegionEndpoint.APSoutheast1;    break; }
                case "apsoutheast2":    { result = RegionEndpoint.APSoutheast2;    break; }
                case "cnnorth1":        { result = RegionEndpoint.CNNorth1;        break; }
                case "eucentral1":      { result = RegionEndpoint.EUCentral1;      break; }
                case "euwest1":         { result = RegionEndpoint.EUWest1;         break; }
                case "saeast1":         { result = RegionEndpoint.SAEast1;         break; }                
                case "useast1":         { result = RegionEndpoint.USEast1;         break; }
                case "usgovcloudwest1": { result = RegionEndpoint.USGovCloudWest1; break; }
                case "uswest1":         { result = RegionEndpoint.USWest1;         break; }
                case "uswest2":         { result = RegionEndpoint.USWest2;         break; }                
            }

            return result;
        }
    }
}
