using Relativity.Kepler.Transport;
using Relativity.Services.DataContracts.DTOs.Results;
using Relativity.Services.Field;
using Relativity.Services.Objects;
using Relativity.Services.Objects.DataContracts;
using Relativity.Services.ServiceProxy;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Fest2018ExportApiWorkshop
{
    class Program
    {
        public Uri RelativityUrl { get; set; }
        public Credentials Credentials { get; set; }
        public int WorkspaceId { get; set; } = 0;
        public int BlockSize { get; set; } = 1000;
        public QueryRequest QueryRequest { get; set; }

        // Utility

        // State

        // Indicator that the text is not present and needs to be streamed
        private const string _SHIBBOLETH = "#KCURA99DF2F0FEB88420388879F1282A55760#";

        static void Main(string[] args)
        {
            Program program = new Program()
            {
                RelativityUrl = new Uri(args[0]),
                WorkspaceId = 1017367,
                QueryRequest = new QueryRequest()
                {
                    Fields = new Relativity.Services.Objects.DataContracts.FieldRef[]
                    {
                        new Relativity.Services.Objects.DataContracts.FieldRef {Name = "Control Number"},
                        new Relativity.Services.Objects.DataContracts.FieldRef {Name = "Extracted Text"}

                    },
                    MaxCharactersForLongTextValues = 1024,
                    ObjectType = new ObjectTypeRef { ArtifactTypeID = 10 } 
        },
                Credentials = new UsernamePasswordCredentials("me@mycompany.com", "MyPasswordDontDoThis987$"),
                BlockSize = 10
            };

            program.Run(args);
        }

        void Run(string[] args)
        {

            // Lets catch all exceptions because it's reasonable practice
            // in a language without checked exceptions

            try
            {
                // Get an instance if the Object Manager

                IObjectManager objectManager;

                try
                {
                    objectManager = GetKeplerServiceFactory()
                        .CreateProxy<Relativity.Services.Objects.IObjectManager>();
                }
                catch (Exception exception)
                {
                    Console.WriteLine(exception.Message);
                    return;
                }

                // Initialize Export API using the properties set above

                Guid runId;
                long recordCount;
                List<FieldMetadata> fieldData;
                int[] longTextIds;

                try
                {
                    ExportInitializationResults exportInitializationResults =
                        objectManager.InitializeExportAsync(WorkspaceId, QueryRequest, 0).Result;

                    // Save infomation about this "run"

                    runId = exportInitializationResults.RunID;
                    recordCount = exportInitializationResults.RecordCount;
                    fieldData = exportInitializationResults.FieldData;

                    // Find indexes of all long text fields

                    List<int> longTextIdList = new List<int>();

                    for (int i = 0; i < exportInitializationResults.FieldData.Count; i++)
                    {
                        if (exportInitializationResults.FieldData[i].FieldType == Relativity.Services.FieldType.LongText)
                        {
                            longTextIdList.Add(i);
                        }
                    }

                    longTextIds = longTextIdList.ToArray();
                }
                catch (Exception exception)
                {
                    Console.WriteLine(exception.Message);
                    return;
                }

                Console.WriteLine("RunId " + runId + " will return " + recordCount + " documents");

                // Get blocks of documents until no more left

                RelativityObjectSlim[] currentBlock = null;
                bool done = false;

                while (!done)
                {
                    try
                    {
                        currentBlock = objectManager.RetrieveNextResultsBlockFromExportAsync(WorkspaceId, runId, BlockSize).Result;
                    }
                    catch (Exception exception)
                    {
                        Console.WriteLine(exception.Message);
                        return;
                    }

                    if (currentBlock == null || !currentBlock.Any())
                    {
                        done = true;
                        break;
                    }

                    Console.WriteLine("Got block of " + currentBlock.Count() + " documents");

                    foreach (RelativityObjectSlim ros in currentBlock)
                    {
                        for (int i = 0; i < fieldData.Count; i++)
                        {
                            Console.WriteLine(fieldData[i].Name + ": " + ros.Values[i]);

                            /*
                            if (longTextIds.Contains(i))
                            {
                                if (ros.Values[i].Equals(_SHIBBOLETH))
                                {
                                    Console.WriteLine("Text is too long, it must be streamed");

                                    RelativityObjectRef documentObjectRef = new RelativityObjectRef { ArtifactID = ros.ArtifactID };
                                    IKeplerStream keplerStream = _objectManager.StreamLongTextAsync(WorkspaceId, documentObjectRef, QueryRequest.Fields.ElementAt(i)).Result;
                                    Stream realStream = keplerStream.GetStreamAsync().Result;

                                    StreamReader reader = new StreamReader(realStream, Encoding.Unicode);
                                    String line;

                                    while ((line = reader.ReadLine()) != null)
                                    {
                                        Console.Write(line);
                                    }
                                }
                            }
                            */
                        }

                        Console.WriteLine();
                    }

                    Console.WriteLine("Complete");
                }
            }
            catch (Exception exception)
            {
                Console.WriteLine(exception.Message);
                return;
            }
        }

        private ServiceFactory GetKeplerServiceFactory()
        {
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            ServicePointManager.DefaultConnectionLimit = 128;

            Uri restUri = new Uri(RelativityUrl, "Relativity.REST/api");
            Uri servicesUri = new Uri(RelativityUrl, "Relativity.REST/apiRelativity.Services");
            ServiceFactorySettings settings = new ServiceFactorySettings(servicesUri, restUri, Credentials);
            ServiceFactory factory = new ServiceFactory(settings);
            return factory;
        }
    }
}
