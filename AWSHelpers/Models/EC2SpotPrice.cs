using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AWSHelpers.Models
{
    public class EC2SpotPrice
    {
        public string   productDescription;
        public float    price; 
        public DateTime timestamp; 
    }
}
