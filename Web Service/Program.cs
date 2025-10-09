using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;

namespace Web_Service // Note: actual namespace depends on the project name.
{
    internal class Program
    {
        // Modelos para mapear los datos de SQL

        static async Task Main(string[] args)
        {
            string sqlQuery = @"WITH FirstProcessName AS(
                            SELECT RIGHT(catalogueId,6) first_process_name
                          FROM Process p
                          INNER JOIN ProcessRevision pr ON pr.masterRef = p.id_Table
                          INNER JOIN ProcessOccurrence po ON pr.id_Table = po.instancedRef
                          WHERE po.parentRef IS NULL
                        )

            SELECT p.catalogueId AS codigo, uud2.value, COALESCE(sq2.name, '') AS Nombre_WA,
            pr.revision AS revEstruct,
            CONCAT('Proceso: ', p.catalogueId, ' - ', fpn.first_process_name) AS descripcion,
            1 AS Cantidad,
            'PA' AS tipo,
            '01' AS deposito,
            'UN' AS 'Unidad de Medida',
            fpn.first_process_name AS Process_name,
            pr.subType,
			TRY_CAST(RIGHT(p.catalogueId,5) AS INT) AS Ordenado
            FROM Operation O
            CROSS JOIN FirstProcessName fpn
            INNER JOIN OperationRevision OpR ON OpR.masterRef = o.id_Table
            INNER JOIN ProcessOccurrence po ON po.instancedRef = OpR.id_Table
            --INNER JOIN Form f ON f.name = CONCAT(o.catalogueId, '/', OpR.revision)
            CROSS APPLY (
              SELECT CASE
                       WHEN RIGHT(o.catalogueId, 3) = '-OP'
                            THEN LEFT(o.catalogueId, LEN(o.catalogueId) - 3)  -- quita ""-OP""
                       WHEN CHARINDEX('-OP', o.catalogueId) > 0
                            THEN REPLACE(o.catalogueId, '-OP', '')             -- más defensivo
                       ELSE o.catalogueId
                     END AS op_code_base
            ) AS x
			LEFT JOIN Form f ON f.name = CONCAT(x.op_code_base, '/', OpR.revision)
            INNER JOIN Form f2 ON f2.id_Table = f.id_Table + 3
            INNER JOIN UserValue_UserData uud ON uud.id_Father = f2.id_Table + 1 AND uud.title = 'allocated_time'
            INNER JOIN ProcessOccurrence po2 ON po2.id_Table = po.parentRef
			LEFT JOIN UserValue_UserData uud2 ON uud2.id_Father = po2.id_Table + 2 AND uud2.title = 'SequenceNumber'
            INNER JOIN ProcessRevision pr ON pr.id_Table = po2.instancedRef
            INNER JOIN Process p ON p.id_Table = pr.masterRef
            LEFT JOIN(SELECT p.catalogueId, sq1.productId, sq1.name FROM Process p
            INNER JOIN ProcessRevision pr ON pr.masterRef = p.id_Table
            INNER JOIN ProcessOccurrence po ON po.instancedRef = pr.id_Table
            INNER JOIN Occurrence o ON po.id_Table = o.parentRef
            INNER JOIN (SELECT p.productId, o.parentRef, '' AS name  FROM Product p
            INNER JOIN ProductRevision pr ON pr.masterRef = p.id_Table
            INNER JOIN Occurrence o ON o.instancedRef = pr.id_Table
            WHERE o.subType IS NULL
			

            UNION ALL

            SELECT wa.catalogueId, wao.parentRef, war.name FROM WorkArea wa
            INNER JOIN WorkAreaRevision war ON war.masterRef = wa.id_Table
            INNER JOIN Occurrence wao ON wao.instancedRef = war.id_Table) sq1 ON sq1.parentRef = o.parentRef) sq2 ON sq2.catalogueId = p.catalogueId


            UNION ALL

            SELECT productId, 0 as value, '' as name, pr.revision, pr.name, COUNT(productId) AS Cantidad,
            'PA' AS tipo,
            '01' AS deposito,
            'UN' AS unMedida,
            fpn.first_process_name AS Process_name,
            pr.subType,
			0 as ordenado
            FROM Product p
            INNER JOIN ProductRevision pr ON pr.masterRef = p.id_Table
            INNER JOIN Occurrence o ON o.instancedRef = pr.id_Table
            CROSS JOIN FirstProcessName fpn
            WHERE o.subType = 'MEConsumed' OR pr.subType LIKE '%MatPrima%'
            GROUP BY productId, pr.revision, pr.name, fpn.first_process_name, pr.subType
			ORDER BY uud2.value DESC, Nombre_WA DESC, TRY_CAST(RIGHT(p.catalogueId,5) AS INT) DESC";

            //            string sqlQuery = @"WITH FirstProcessName AS (
            //                SELECT RIGHT(catalogueId,6) first_process_name
            //              FROM Process p
            //              INNER JOIN ProcessRevision pr ON pr.masterRef = p.id_Table
            //              INNER JOIN ProcessOccurrence po ON pr.id_Table = po.instancedRef
            //              WHERE po.parentRef IS NULL
            //            )

            //SELECT p.catalogueId AS codigo, 
            //pr.revision AS revEstruct,
            //CONCAT('Proceso: ', p.catalogueId, ' - ', fpn.first_process_name) AS descripcion,
            //1 AS Cantidad,
            //'PA' AS tipo,
            //'01' AS deposito,
            //'UN' AS unMedida,
            //fpn.first_process_name AS Process_name,
            //pr.subType
            //FROM Operation O
            //CROSS JOIN FirstProcessName fpn
            //INNER JOIN OperationRevision OpR ON OpR.masterRef = o.id_Table
            //INNER JOIN ProcessOccurrence po ON po.instancedRef = OpR.id_Table
            //INNER JOIN Form f ON f.name = CONCAT(o.catalogueId,'/',OpR.revision)
            //INNER JOIN Form f2 ON f2.id_Table = f.id_Table + 3
            //INNER JOIN UserValue_UserData uud ON uud.id_Father = f2.id_Table + 1 AND uud.title = 'allocated_time'
            //INNER JOIN ProcessOccurrence po2 ON po2.id_Table = po.parentRef
            //INNER JOIN ProcessRevision pr ON pr.id_Table = po2.instancedRef
            //INNER JOIN Process p ON p.id_Table = pr.masterRef
            //INNER JOIN ( SELECT p.catalogueId, sq1.productId FROM Process p
            //INNER JOIN ProcessRevision pr ON pr.masterRef = p.id_Table
            //INNER JOIN ProcessOccurrence po ON po.instancedRef = pr.id_Table
            //INNER JOIN Occurrence o ON po.id_Table = o.parentRef
            //INNER JOIN ( SELECT p.productId, o.parentRef FROM Product p
            //INNER JOIN ProductRevision pr ON pr.masterRef = p.id_Table
            //INNER JOIN Occurrence o ON o.instancedRef = pr.id_Table
            //WHERE o.subType IS NULL

            //UNION ALL

            //SELECT wa.catalogueId, wao.parentRef FROM WorkArea wa
            //INNER JOIN WorkAreaRevision war ON war.masterRef = wa.id_Table
            //INNER JOIN Occurrence wao ON wao.instancedRef = war.id_Table) sq1 ON sq1.parentRef = o.parentRef) sq2 ON sq2.catalogueId = p.catalogueId

            //UNION ALL

            //SELECT productId, pr.revision, pr.name, COUNT(productId) AS Cantidad,
            //'PA' AS tipo,
            //'01' AS deposito,
            //'UN' AS unMedida,
            //fpn.first_process_name AS Process_name,
            //pr.subType
            //FROM Product p
            //INNER JOIN ProductRevision pr ON pr.masterRef = p.id_Table
            //INNER JOIN Occurrence o ON o.instancedRef = pr.id_Table
            //CROSS JOIN FirstProcessName fpn
            //WHERE o.subType = 'MEConsumed'
            //GROUP BY productId, pr.revision, pr.name, fpn.first_process_name, pr.subType";


            string connectionString = @"Data Source=DEPLM-11-PC\SQLEXPRESS;Initial Catalog=AgrometalBop;
                                        Integrated Security=True;TrustServerCertificate=True";

            var converter = new SqlToJsonConverter(connectionString);

            XmlDocument xmlDoc = new XmlDocument();

            string nameCarpeta = @"E:\a\Rodrigo Bertero\Prueba";

            if (Directory.Exists(nameCarpeta))
            {

                string[] archivos = Directory.GetFiles(nameCarpeta);
                int contadorXmls = 1;

                // Por cada archivo XML, este es subido a la base de datos con su identificador xml

                bool ban = true;
                foreach (string archivo in archivos)
                {
                    try
                    {
                        xmlDoc.Load(archivo); //Cargar el xml                           
                        using (SqlConnection connection = new SqlConnection(connectionString))
                        {
                            connection.Open();
                            
                            XmlNode root = xmlDoc.DocumentElement;
                            Dictionary<string, List<DataRow>> groupedDataRows = new Dictionary<string, List<DataRow>>();

                            if (ParseNode(root, groupedDataRows))
                            {
                                //if (ban)
                                //{
                                //    BorrarTabla(connection, groupedDataRows);
                                //    //ban = false;
                                //}

                                CreateTable(connection, groupedDataRows);
                                InsertData(connection, groupedDataRows, archivo, contadorXmls);

                                // SB1: Productos
                                List<string> listaSB1 = new List<string>();
                                listaSB1 = Tablas_SG2_SH3.jsonSB1_BOP();
                                foreach (string s in listaSB1)
                                {
                                    //Console.WriteLine(s);
                                    await Tabla_SB1.postSB1(s);
                                }

                                // SG1: Estructuras
                                Dictionary<string, List<List<Dictionary<string, string>>>> estructuras = new Dictionary<string, List<List<Dictionary<string, string>>>>();
                                estructuras = Tabla_SG1.jsonSG1();
                                await Tabla_SG1.postSG1(estructuras);

                                // SG2/SH3: Procesos
                                List<string> listaSG2 = new List<string>();
                                listaSG2 = Tablas_SG2_SH3.jsonSG2_SH3();
                                Console.WriteLine($"[DEBUG] SG2/SH3: {listaSG2?.Count ?? 0} items generados");
                                foreach (string s in listaSG2)
                                {
                                    Console.WriteLine(s);
                                    await Tablas_SG2_SH3.postSG2_SH3(s);
             
                                }


                                //foreach (string s in listaSB1)
                                //{
                                //    Console.WriteLine(s);
                                //    await Tabla_SB1.postSB1(s);
                                //}
                                //listaSB1 = Tablas_SG2_SH3.jsonSG2_SH3();
                                //foreach (string s in listaSB1)
                                //{
                                //    Console.WriteLine(s);
                                //    await Tablas_SG2_SH3.postSG2_SH3(s);
                                //}


                                //Dictionary<string, List<List<Dictionary<string, string>>>> estructuras = new Dictionary<string, List<List<Dictionary<string, string>>>>();
                                //estructuras = Tabla_SG1.jsonSG1();
                                //await Tabla_SG1.postSG1(estructuras);

                                try
                                {
                                    // Opción 1: Mostrar estructura jerárquica
                                    Console.WriteLine(" === ESTRUCTURA JERÁRQUICA ===");
                                    converter.ShowHierarchicalStructure(sqlQuery);

                                    // Opción 2: Mostrar cadena jerárquica completa
                                    Console.WriteLine("\n=== CADENA JERÁRQUICA ===");
                                    converter.ShowHierarchicalChain(sqlQuery);

                                    // Opción 3: Procesar y mostrar todos los JSONs
                                    Console.WriteLine("\n=== JSONs INDIVIDUALES ===");
                                    converter.ProcessHierarchicalJsons(sqlQuery);

                                    // Opción 4: Obtener lista de JSONs como strings
                                    Console.WriteLine("\n=== OBTENIENDO LISTA DE JSONs ===");
                                    var jsonStrings = converter.ConvertToHierarchicalJsonStrings(sqlQuery);
                                    string apiUrl = "http://119.8.73.193:8086/rest/TCEstructura/Incluir/";
                                    string username = "USERREST";
                                    string password = "restagr";

                                    Tabla_SG1 tabla_SG1 = new Tabla_SG1();
                                    //Tablas_SG2_SH3.jsonSB1_BOP();
                                    //for (int i = jsonStrings.Count - 1; i == 0; i--)
                                    //{
                                    //    await tabla_SG1.postSG1(jsonStrings[i]);
                                    //}
                                    foreach (string s in jsonStrings)
                                    {

                                        await tabla_SG1.postSG1(s);
                                    }
                                    //List<string> responses = await tabla_SG1.PostSG1(apiUrl, jsonStrings, username, password);
                                    //string firstResponse = responses.FirstOrDefault() ?? "No se recibió respuesta";


                                    Console.WriteLine($"Se generaron {jsonStrings.Count} JSONs jerárquicos");

                                    // Mostrar todos los JSONs
                                    for (int i = 0; i < jsonStrings.Count; i++)
                                    {
                                        Console.WriteLine($"\nJSON #{i + 1}:");
                                        Console.WriteLine(jsonStrings[i]);
                                    }

                                    // Opción 5: Guardar en archivos separados
                                    Console.WriteLine("\n=== GUARDANDO ARCHIVOS ===");
                                    //converter.SaveHierarchicalJsonFiles(sqlQuery, @"C:\Agrometal\jsons\");

                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"Error: {ex.Message}");
                                }

                                // ------------------------------------ PROCESOS ENTERO ----------------------
                            }

                            groupedDataRows.Clear();
                            //contadorXmls++;
                        }
                        Utilidades.EscribirEnLog($"Archivo XML: {Path.GetFileName(archivo)} cargado correctamente");
                    }
                    catch (Exception ea)
                    {
                        Utilidades.EscribirEnLog($"Error al cargar el XML: {Path.GetFileName(archivo)} \nError: {ea.Message} ");
                    }

                }
            }


            //string ruta = @"E:\Proyectos\Agrometal\Importacion\020597prt.jpg";
            //string ruta2 = @"E:\Proyectos\Agrometal\Importacion\CONVERTIDOOO2.jpg";
            //string valor = ConvertImageToBase64(ruta);
            //string valor2 = @"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ldZqHjnw5p6AnU4bmRlYpFaHzmYgZx8uQCe24gfka5y8+KEj/LpmiSlWjO2W8lEex+eqLkkdO4z7daSv0FHm6HV6vfW/hfw0TbiNBBGsFrHI2V3YwucnJUAZbnO1WPavGkUedNNvmfzGOGmcsxGTySe7Esx6csa0NY1zWPETwnUriGOKMHFtbIQhJPLHcSSSOPoSO5zTqkrGkY2CiiiqKCiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKK0Ybu2W2hQnY6qQ/wDokcm47ic5Y56ED8Kd9stv+e3/AJTof8aYGZRWn9stv+e3/lOh/wAagvbiCaKJYuXVmLP5CRZBxgYU89D+dAFOiiikAUUUUAFFFFABRRRQAUUUUAFFFFABRRRQAUUUUAFFFFABRRRQAV1c+lXJuJDFEBHuO0CzgIAzxyWz+dcpWvPqFtNcSSCQqHYsAbCJiMn1J5+tNAaK6TcncJICysrLgWsCkEggHIfPB5/CuevLV7O6khY5CsQrdmAOM/p+HSry31sm4lhIdrBVNhEo3YODkHscGs+e4kuGUybflXaoVAoAyT0AA6k0MCKiiikB/9k=";
            //bool valor3 = SaveBase64ToImageFile(valor2, ruta2);

            //Console.WriteLine(valor);

            // Aquí puedes continuar con el procesamiento si la carga fue exitosa


            //List<string> listaSB1 = new List<string>();
            //listaSB1 = Tablas_SG2_SH3.jsonSG2_SH3();
            //listaSB1 = Tabla_SB1.jsonSB1();

            //foreach (string s in listaSB1)
            //{
            //    await Tabla_SB1.putSB1(s);
            //}

            //Dictionary<string, List<List<Dictionary<string, string>>>> estructuras = new Dictionary<string, List<List<Dictionary<string, string>>>>();
            //estructuras = Tabla_SG1.jsonSG1();
            //await Tabla_SG1.postSG1(estructuras);

            //Tablas_SG2_SH3.jsonSG2_SH3();

            //listaSB1 = Tablas_SG2_SH3.jsonSB1_BOP();
            //string json = @"
            //                {
            //                  ""producto"": [
            //                    {
            //                      ""campo"": ""codigo"",
            //                      ""valor"": ""300023""
            //                    },
            //                    {
            //                      ""campo"": ""descripcion"",
            //                      ""valor"": ""KITS G.G.""
            //                    },
            //                    {
            //                      ""campo"": ""tipo"",
            //                      ""valor"": ""PA""
            //                    },
            //                    {
            //                      ""campo"": ""deposito"",
            //                      ""valor"": ""01""
            //                    },
            //                    {
            //                      ""campo"": ""unMedida"",
            //                      ""valor"": ""UN""
            //                    }
            //                  ]
            //                }";

            //foreach (string s in listaSB1)
            //{
            //    //await Tabla_SB1.postSB1(s);
            //    await Tablas_SG2_SH3.postSG2_SH3(s);
            //    Console.WriteLine(s);
            //}

            //try
            //{
            //    var jsonCompleto = await PruebaSG1.GenerarJSONCompleto();
            //    Console.WriteLine("JSON generado exitosamente:");
            //    Console.WriteLine(jsonCompleto);
            //}
            //catch (Exception ex)
            //{
            //    Console.WriteLine($"Error: {ex.Message}");
            //    Console.WriteLine($"Stack trace: {ex.StackTrace}");
            //}
            //Console.WriteLine(jsonCompleto.Result.ToString());

            //Tabla_SB1.jsonSB1();
            //Tabla_SG1.jsonSG1();

            //string connectionString = "Server=DEPLM-07-PC\\SQLEXPRESS;Database=AgrometalBop;Trusted_Connection=True;";
            //string outputDirectory = @"C:\Agrometal\Agrometal pim quintos"; // Cambiar por tu directorio

            //try
            //{
            //    ProcessFileGenerator generator = new ProcessFileGenerator(connectionString, outputDirectory);
            //    generator.GenerateProcessFiles();

            //    Console.WriteLine("Proceso completado exitosamente.");
            //}
            //catch (Exception ex)
            //{
            //    Console.WriteLine($"Error en el proceso: {ex.Message}");
            //}

            //Console.WriteLine("Presiona cualquier tecla para salir...");
            //Console.ReadKey();

        }

        public static string ConvertImageToBase64(string imagePath)
        {
            try
            {
                byte[] imageBytes = File.ReadAllBytes(imagePath);
                return Convert.ToBase64String(imageBytes);
            }
            catch (Exception ex)
            {
                throw new Exception($"Error al convertir imagen a Base64: {ex.Message}");
            }
        }

        public static bool SaveBase64ToImageFile(string base64String, string outputPath)
        {
            try
            {
                byte[] imageBytes = Convert.FromBase64String(base64String);
                File.WriteAllBytes(outputPath, imageBytes);
                return true;
            }
            catch (Exception ex)
            {
                throw new Exception($"Error al guardar imagen desde Base64: {ex.Message}");
            }
        }

        static Dictionary<string, string> Workarea(string connectionString) //Esto obtiene el workarea del proceso
        {
            string sqlworkarea = @"SELECT
	                                guest.Process.catalogueId AS instancedProcess,
	                                guest.WorkArea.catalogueId AS instancedWorkArea        
	                                FROM
		                                guest.ProcessOccurrence
	                                CROSS APPLY (
		                                SELECT TRIM(value) AS FormID, ProcessOccurrence.id_Table
		                                FROM STRING_SPLIT(ProcessOccurrence.occurrenceRefs, ' ')
		                                WHERE value <> ''
	                                ) AS squary1
	                                JOIN guest.Occurrence AS occ ON TRY_CAST(SUBSTRING(squary1.FormID, 3, LEN(squary1.FormID)) AS INT) = Occ.parentRef and occ.idXml = ProcessOccurrence.idXml
	                                JOIN guest.ProcessRevision ON ProcessRevision.id_Table = ProcessOccurrence.instancedRef and ProcessRevision.idXml = ProcessOccurrence.idXml
	                                JOIN guest.Process ON ProcessRevision.masterRef = Process.id_Table and ProcessRevision.idXml = Process.idXml
	                                JOIN guest.WorkAreaRevision ON WorkAreaRevision.id_Table = occ.instancedRef and WorkAreaRevision.idXml = occ.idXml
	                                JOIN guest.WorkArea ON WorkAreaRevision.masterRef = WorkArea.id_Table and WorkAreaRevision.idXml = WorkArea.idXml
	                                WHERE
	                                occ.subType = 'MEWorkArea'
	                                AND ProcessOccurrence.id_Table = squary1.id_Table";

            Dictionary<string, string> resultDictionary = new Dictionary<string, string>();

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    SqlCommand command = new SqlCommand(sqlworkarea, connection);
                    SqlDataReader reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        // Leer valores de las columnas
                        string instancedProcess = reader.GetString(0);
                        string instancedWorkArea = reader.GetString(1);

                        // Agregar al diccionario
                        resultDictionary[instancedProcess] = instancedWorkArea;
                    }

                    reader.Close();
                }
                catch (Exception ex)
                {
                    Utilidades.EscribirEnLog("Error en la consulta de WorkArea: " + ex.Message);
                }
            }

            return resultDictionary;

        }
        static List<Dictionary<string, string>> MbomBop(string connectionString) //Relación MBOM-BOP con General Relation
        {
            string sqlRelated = @"select distinct squary1.FormID, CAST(squary1.id_Table as INT),GeneralRelation.relatedRefs,case when ProductRevision.id_Table is not null then Product.productId
		                                when guest.ProductRevision.id_Table is null then Process.catalogueId end , GeneralRelation.idXml                                                                
		                                from
		                                guest.GeneralRelation
		                                cross apply (SELECT TRIM(value) AS FormID, GeneralRelation.id_Table
		                                FROM STRING_SPLIT(GeneralRelation.relatedRefs, ' ') WHERE value <> '') AS squary1
		                                LEFT JOIN guest.ProductRevision ON TRY_CAST(SUBSTRING(squary1.FormID, 4, LEN(squary1.FormID)) AS INT) = ProductRevision.id_Table and GeneralRelation.idXml = ProductRevision.idXml
		                                LEFT JOIN guest.ProcessRevision ON TRY_CAST(SUBSTRING(squary1.FormID, 4, LEN(squary1.FormID)) AS INT) = ProcessRevision.id_Table and GeneralRelation.idXml = ProcessRevision.idXml

		                                left join guest.Product on Product.id_Table = ProductRevision.masterRef and Product.idXml = ProductRevision.idXml
		                                left join guest.Process on Process.id_Table = ProcessRevision.masterRef and Process.idXml = ProcessRevision.idXml

		                                where GeneralRelation.id_Table = squary1.id_Table
		                                order by GeneralRelation.idXml ASC";
            List<Dictionary<string, string>> listRelatedMbomBop = new List<Dictionary<string, string>>();

            using (SqlConnection connection = new SqlConnection(connectionString))
            {

                connection.Open();
                SqlCommand command3 = new SqlCommand(sqlRelated, connection);
                SqlDataReader reader3 = command3.ExecuteReader();

                try
                {
                    int idtableAux = 0;
                    int idXmlAux = 0;
                    string codigoAux = string.Empty;

                    while (reader3.Read())
                    {
                        if (reader3.GetInt32(1) == idtableAux && reader3.GetInt32(4) == idXmlAux)
                        {
                            string valCodigo = reader3.IsDBNull(3) ? string.Empty : reader3.GetString(3);

                            Dictionary<string, string> diccionario1 = new Dictionary<string, string>
                                                {
                                                    { valCodigo, codigoAux }
                                                };
                            listRelatedMbomBop.Add(diccionario1);

                            codigoAux = string.Empty;
                        }
                        else
                        {
                            codigoAux = reader3.IsDBNull(3) ? string.Empty : reader3.GetString(3);
                        }

                        idtableAux = reader3.GetInt32(1);
                        idXmlAux = reader3.GetInt32(4);
                    }
                }
                catch (Exception ea)
                {
                    Utilidades.EscribirEnLog("Error en la consulta, related MBOM - BOP: " + ea.Message);
                }
                finally
                {
                    command3.Cancel();
                    reader3.Close();
                    command3.Dispose();
                }
            }

            return listRelatedMbomBop;
        }
        static void LimpiarCarpeta(string carpeta) //Limpia la carpeta donde se exportan todos los Xmls, uno por uno antes de volver a exportar más de otra estructura
        {
            try
            {


                if (Directory.Exists(carpeta))
                {
                    // Borra todos los archivos en la carpeta
                    string[] archivos = Directory.GetFiles(carpeta);
                    foreach (string archivo in archivos)
                    {
                        File.Delete(archivo);
                    }

                    Utilidades.EscribirEnLog($"Carpeta {carpeta} limpiada correctamente.");
                }
            }
            catch (Exception ex)
            {
                // Maneja cualquier excepción que pueda ocurrir durante la limpieza
                Utilidades.EscribirEnLog($"Error al limpiar la carpeta: {ex.Message}");
            }
        }
        //public static void limpiarPlanilla(string rutaExcel, int filaDeComienzo)
        //{
        //    // Establecer el contexto de la licencia de EPPlus
        //    ExcelPackage.LicenseContext = LicenseContext.NonCommercial; // O Commercial si tienes una licencia comercial                
        //    using (var excelPackage = new ExcelPackage(new System.IO.FileInfo(rutaExcel)))
        //    {
        //        // Iterar sobre todas las hojas del libro
        //        foreach (var worksheet in excelPackage.Workbook.Worksheets)
        //        {
        //            // Encontrar la última fila con datos en la hoja de cálculo
        //            int lastRow = worksheet.Dimension.End.Row;
        //            // Encontrar la última columna con datos en la hoja de cálculo
        //            int lastColumn = worksheet.Dimension.End.Column;

        //            // Borrar el contenido desde la fila especificada hasta la última fila y columna con datos
        //            for (int row = filaDeComienzo; row <= lastRow; row++)
        //            {
        //                for (int col = 1; col <= lastColumn; col++)
        //                {
        //                    worksheet.Cells[row, col].Value = null;
        //                }
        //            }
        //        }

        //        // Guardar los cambios en el archivo Excel
        //        excelPackage.Save();
        //    }

        //    Console.WriteLine("Planilla limpiada exitosamente en todas las hojas.");
        //}

        // --------------------------------------------- Metodos utilizados por el Main() Carga en Base de datos de BOP ---------------------------------------------
        static void BorrarTabla(SqlConnection connection, Dictionary<string, List<DataRow>> groupedDataRows)
        {
            foreach (var group in groupedDataRows)
            {
                try
                {
                    string tableName = group.Key;
                    string deleteTableQuery = $"IF OBJECT_ID('[{tableName}]', 'U') IS NOT NULL DROP TABLE [{tableName}]";
                    using (SqlCommand command = new SqlCommand(deleteTableQuery, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                }
                catch (Exception ea)
                {
                    Utilidades.EscribirEnLog($"Error al intentar borrar la tabla para su sobreescritura - Error: {ea.Message}");
                }
            }
        }

        static bool ParseNode(XmlNode node, Dictionary<string, List<DataRow>> groupedDataRows, string parentNodeName = "")
        {
            // Crear una lista de nombres de nodos a ignorar
            var listaIgnorados = new List<string> { "ApplicationRef", "AssociatedDataSet", "AttributeContext", "DataSet",
                                                        "ExternalFile", "Folder", "InstanceGraph", "ProductDef", "ProductInstance",
                                                        "ProductRevisionView", "RevisionRule", "Site", "Transform", "View" };
            try
            {
                if (node.NodeType == XmlNodeType.Element && !listaIgnorados.Contains(node.Name))
                {
                    string nodeName = node.Name; //Nombre actual del nodo

                    DataRow dataRow = new DataRow(); //Nuevo objeto datarow
                    dataRow.NombreNodo = nodeName;


                    dataRow.Atributos = new List<string>();

                    foreach (XmlAttribute attribute in node.Attributes)
                    {
                        dataRow.Atributos.Add(attribute.Name); //Guarda los nombres de los atributos
                    }

                    dataRow.XmlNode = node;
                    string tableName = GetTableName(nodeName, dataRow.Atributos, parentNodeName); //Creacion de nombre de la tabla

                    if (!groupedDataRows.ContainsKey(tableName))
                    {
                        groupedDataRows[tableName] = new List<DataRow>();
                    }
                    groupedDataRows[tableName].Add(dataRow);

                    foreach (XmlNode childNode in node.ChildNodes)
                    {
                        ParseNode(childNode, groupedDataRows, nodeName); //recursividad
                    }
                    return true;
                }
                return false;
            }
            catch (Exception ea)
            {
                Utilidades.EscribirEnLog("Excepcion controlada en el metodo ParseNode: " + ea.Message);
                return false;
            }

        }

        static string GetTableName(string nodeName, List<string> attributes, string parentNodeName)
        {
            string tableName = nodeName;
            if (!attributes.Contains("id") && tableName != "PLMXML") //Si no tiene el atributo id y no es el nodo PLMXML
            {

                tableName = $"{nodeName}_{parentNodeName}";
            }
            return tableName;
        }

        static void CreateTable(SqlConnection connection, Dictionary<string, List<DataRow>> groupedDataRows)
        {
            try
            {
                foreach (var group in groupedDataRows)
                {

                    string tableName = group.Key;
                    if (tableName == "PLMXML") // Saltar el nodo "PLMXML"
                    {
                        continue;
                    }
                    string createTableQuery = $"IF OBJECT_ID('[{tableName}]', 'U') IS NULL CREATE TABLE [{tableName}] (id INT IDENTITY(1,1) PRIMARY KEY, contenido NVARCHAR(MAX)";
                    List<string> additionalAttributes = new List<string>();
                    bool hasIdAttribute = false;

                    foreach (DataRow dataRow in group.Value)
                    {
                        foreach (string attribute in dataRow.Atributos)
                        {
                            if (!additionalAttributes.Contains(attribute) && attribute != "id") //Adicional atributo
                            {
                                additionalAttributes.Add(attribute);
                            }
                            if (attribute == "id") //Existe el atributo id
                            {
                                hasIdAttribute = true;
                            }
                        }
                    }

                    if (hasIdAttribute) // Existe el atributo "id", agregar id_Table
                    {
                        createTableQuery += ", id_Table NVARCHAR(MAX) ";
                    }
                    else // No existe el atributo "id", agregar id_Father
                    {
                        createTableQuery += ", id_Father NVARCHAR(MAX) ";
                    }
                    foreach (string columnName in additionalAttributes) //Creacion de columnas
                    {
                        if (columnName != "id")
                        {
                            createTableQuery += $", [{columnName}] NVARCHAR(MAX)";
                        }
                    }
                    createTableQuery += ", idXml INT);";
                    using (SqlCommand command = new SqlCommand(createTableQuery, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ea)
            {
                Utilidades.EscribirEnLog($"Excepcion controlada en el metodo createTable: {ea.Message}");
                throw;
            }

        }

        static void AlterTable(SqlConnection connection, string tableName, string columnName, string columnType)
        {
            try
            {
                string alterTableQuery = $"IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableName}' AND COLUMN_NAME = '{columnName}') " +
                $"ALTER TABLE [{tableName}] ADD [{columnName}] {columnType};";

                using (SqlCommand command = new SqlCommand(alterTableQuery, connection))
                {
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ea)
            {
                Utilidades.EscribirEnLog($"Excepcion controlada en el metodo AlterTable: {ea.Message}");
                throw;
            }

        }

        static void InsertData(SqlConnection connection, Dictionary<string, List<DataRow>> groupedDataRows, string xml, int contadorXmls)
        {
            try
            {
                foreach (var group in groupedDataRows)
                {
                    string tableName = group.Key;

                    foreach (DataRow dataRow in group.Value)
                    {
                        if (dataRow.NombreNodo == "PLMXML") // Saltar el nodo "PLMXML"
                            continue;
                        string insertQuery = $"INSERT INTO [{tableName}] (";
                        List<string> columnNames = new List<string>();
                        List<string> parameterNames = new List<string>();
                        List<SqlParameter> parameters = new List<SqlParameter>();
                        bool hasIdAttribute = false;


                        foreach (string columnName in dataRow.Atributos)
                        {

                            if (columnName == "id" || columnName == "instancedRef" || columnName == "masterRef" || columnName == "parentRef" || columnName == "instanceRefs") //Columna id_Table, instancedRef y masterRef
                            {
                                string attributeValue1 = dataRow.XmlNode.Attributes[columnName]?.Value;

                                if (columnName == "id" && !string.IsNullOrEmpty(attributeValue1) && attributeValue1.Length > 2)
                                {
                                    hasIdAttribute = true;
                                    columnNames.Add("[id_Table]");
                                    parameterNames.Add("@id");
                                    attributeValue1 = attributeValue1.Substring(2); //Suprimir los dos primeros caracteres
                                    parameters.Add(new SqlParameter("@id", attributeValue1));
                                }
                                if (columnName == "instancedRef" && !string.IsNullOrEmpty(attributeValue1) && attributeValue1.Length > 2)
                                {
                                    columnNames.Add("[instancedRef]");
                                    parameterNames.Add("@instancedRef");
                                    attributeValue1 = attributeValue1.Substring(3); //Suprimir los dos primeros caracteres
                                    parameters.Add(new SqlParameter("@instancedRef", attributeValue1));
                                }
                                if (columnName == "masterRef" && !string.IsNullOrEmpty(attributeValue1) && attributeValue1.Length > 2)
                                {
                                    columnNames.Add("[masterRef]");
                                    parameterNames.Add("@masterRef");
                                    attributeValue1 = attributeValue1.Substring(3); //Suprimir los dos primeros caracteres
                                    parameters.Add(new SqlParameter("@masterRef", attributeValue1));
                                }
                                if (columnName == "parentRef" && !string.IsNullOrEmpty(attributeValue1) && attributeValue1.Length > 2)
                                {
                                    columnNames.Add("[parentRef]");
                                    parameterNames.Add("@parentRef");
                                    attributeValue1 = attributeValue1.Substring(3); //Suprimir los tres primeros caracteres
                                    parameters.Add(new SqlParameter("@parentRef", attributeValue1));
                                }
                                if (columnName == "instanceRefs" && !string.IsNullOrEmpty(attributeValue1) && attributeValue1.Length > 2)
                                {
                                    columnNames.Add("[instanceRefs]");
                                    parameterNames.Add("@instanceRefs");
                                    attributeValue1 = attributeValue1.Substring(3); //Suprimir los tres primeros caracteres
                                    parameters.Add(new SqlParameter("@instanceRefs", attributeValue1));
                                }
                                continue;
                            }
                            AlterTable(connection, tableName, columnName, "NVARCHAR(MAX)");
                            columnNames.Add($"[{columnName}]"); //Columnas de otros atributos que no son id,contenido y id_father
                            parameterNames.Add($"@{columnName}");
                            string attributeValue = dataRow.XmlNode.Attributes[columnName]?.Value;
                            attributeValue = attributeValue.Replace("'", "''");
                            parameters.Add(new SqlParameter($"@{columnName}", attributeValue));

                        }
                        columnNames.Add("[contenido]");//Columna contenido
                        parameterNames.Add("@contenido");
                        parameters.Add(new SqlParameter("@contenido", dataRow.XmlNode.InnerText));

                        if (!hasIdAttribute) //Columna de tablas sin id
                        {
                            columnNames.Add("[id_Father]");
                            parameterNames.Add("@idFather");
                            XmlNode parentNode = dataRow.XmlNode.ParentNode;
                            string parentAttributeValue = parentNode?.Attributes["id"]?.Value;
                            string parentAttributeId = parentAttributeValue?.Substring(2) ?? "0";
                            parameters.Add(new SqlParameter("@idFather", parentAttributeId));
                        }
                        columnNames.Add("[idXml]"); // Agregar la columna idXml
                        parameterNames.Add("@idXml");
                        parameters.Add(new SqlParameter("@idXml", contadorXmls)); // Insertar el valor de contadorXmls

                        insertQuery += string.Join(", ", columnNames) + ") VALUES (";
                        insertQuery += string.Join(", ", parameterNames) + ");";

                        using (SqlCommand command = new SqlCommand(insertQuery, connection))
                        {
                            command.Parameters.AddRange(parameters.ToArray());
                            command.ExecuteNonQuery();
                        }
                    }
                }
            }
            catch (Exception ea)
            {
                Utilidades.EscribirEnLog($"Excepcion controlada en el metodo InsertData {ea.Message}");
            }

        }
    }
}