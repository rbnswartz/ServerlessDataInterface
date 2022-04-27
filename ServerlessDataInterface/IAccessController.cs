using ServerlessDataInterface.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ServerlessDataInterface
{
    public interface IAccessController
    {
        AccessControlResponse CheckAccess(string recordType, AccessType accessType, string recordId, Dictionary<string, object> fields = null);
    }
    public enum AccessType
    {
        Read,
        Write,
        Delete,
        Create
    }
}
