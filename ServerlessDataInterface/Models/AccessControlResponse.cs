using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ServerlessDataInterface.Models
{
    public class AccessControlResponse
    {
        public bool Allowed { get; set; }
        public bool AllFieldsAllowed { get; set; }
        public List<string> AllowedFields { get; set; }
        public AccessControlResponse()
        {
            AllowedFields = new List<string>();
        }
    }
}
