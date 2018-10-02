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
using FieldRef = Relativity.Services.Objects.DataContracts.FieldRef;

namespace Fest2018ExportApiWorkshop
{
  class Program
  {
    public Uri RelativityUrl { get; set; }
    public Credentials Credentials { get; set; }
    public int WorkspaceId { get; set; } = 0;
    public QueryRequest QueryRequest { get; set; }

    // Indicator that the text is not present and needs to be streamed
    private const string _SHIBBOLETH = "#KCURA99DF2F0FEB88420388879F1282A55760#";

    static void Main(string[] args)
    {
      Program program = new Program()
      {
        RelativityUrl = new Uri("https://fest2018-current-sandbox.relativity.one"),
        WorkspaceId = 1082531,
        Credentials = new UsernamePasswordCredentials("test-user@fest.com", "Password goes here!"),
        QueryRequest = new QueryRequest()
        {
          ObjectType = new ObjectTypeRef
          {
            ArtifactTypeID = 10 // Documents
          },
          Fields = new FieldRef[]
              {
                        new FieldRef {Name = "Control Number"},
                        new FieldRef {Name = "Extracted Text"}

              },
          MaxCharactersForLongTextValues = 1024 * 10
        }
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

        try
        {
          ExportInitializationResults exportInitializationResults =
              objectManager.InitializeExportAsync(WorkspaceId, QueryRequest, 0).Result;

          // Save infomation about this "run"

          runId = exportInitializationResults.RunID;
          recordCount = exportInitializationResults.RecordCount;
          fieldData = exportInitializationResults.FieldData;
        }
        catch (Exception exception)
        {
          Console.WriteLine(exception.Message);
          return;
        }

        Console.WriteLine("RunId " + runId + " will return " + recordCount + " documents");
        Console.WriteLine();

        // Get blocks of documents until no more left

        RelativityObjectSlim[] currentBlock = null;
        bool done = false;

        while (!done)
        {
          try
          {
            currentBlock = objectManager.RetrieveNextResultsBlockFromExportAsync(
              WorkspaceId, runId, 10).Result;
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
          Console.WriteLine();

          // Print out each document's fields 

          foreach (RelativityObjectSlim ros in currentBlock)
          {
            for (int i = 0; i < fieldData.Count; i++)
            {
              Console.WriteLine(fieldData[i].Name + ": " + ros.Values[i]);

              /*
               
              // If this field is long text and it contains
              // only the streaming marker then stream.

              if (fieldData[i].FieldType == Relativity.Services.FieldType.LongText
                  && ros.Values[i].Equals(_SHIBBOLETH))
              {
                Console.WriteLine("Text is too long, it must be streamed");
                Console.WriteLine();

                RelativityObjectRef documentObjectRef = new RelativityObjectRef { ArtifactID = ros.ArtifactID };

                using (IKeplerStream keplerStream = objectManager.StreamLongTextAsync(WorkspaceId, documentObjectRef, QueryRequest.Fields.ElementAt(i)).Result)
                {
                  using (Stream realStream = keplerStream.GetStreamAsync().Result)
                  {
                    StreamReader reader = new StreamReader(realStream, Encoding.Unicode);
                    String line;

                    while ((line = reader.ReadLine()) != null)
                    {
                      Console.Write(line);
                    }
                    Console.WriteLine();
                  }
                }
              }
              */
            }

            Console.WriteLine();
          }

          Console.WriteLine("Block complete");
          Console.WriteLine();
        }

        Console.WriteLine("All blocks complete");
        Console.WriteLine();
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
