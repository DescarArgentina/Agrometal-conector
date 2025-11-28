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

            string connectionString = @"Data Source=DEPLM-11-PC\SQLEXPRESS;Initial Catalog=AgrometalBop;
                                        Integrated Security=True;TrustServerCertificate=True";

            await ProcesarMBOM(connectionString);
            await ProcesarBOP(connectionString);

        }

        private static async Task ProcesarBOP(string connectionString)
        {
            Console.WriteLine("=== INICIANDO PROCESAMIENTO BOP ===");

            string sqlQuery = @"WITH FirstProcessName AS (
        SELECT RIGHT(p.catalogueId, 6) AS first_process_name
        FROM Process p
        INNER JOIN ProcessRevision pr ON pr.masterRef = p.id_Table
        INNER JOIN ProcessOccurrence po ON pr.id_Table = po.instancedRef
        WHERE po.parentRef IS NULL
    )
    SELECT
        fpn.first_process_name AS Process_codigo,          

        CASE 
            WHEN LEFT(p.catalogueId, 1) IN ('M','E')
                THEN RIGHT(p.catalogueId, LEN(p.catalogueId) - 1)
            WHEN RIGHT(p.catalogueId, 3) = '-FV'
                THEN LEFT(p.catalogueId, LEN(p.catalogueId) - 3)
            ELSE p.catalogueId
        END AS PR_Codigo,

        CASE 
            WHEN LEFT(prod.productId, 1) IN ('M','E')
                THEN RIGHT(prod.productId, LEN(prod.productId) - 1)
            WHEN RIGHT(prod.productId, 3) = '-FV'
                THEN LEFT(prod.productId, LEN(prod.productId) - 3)
            ELSE prod.productId
        END AS Codigo,

        COUNT(prod.productId) AS Cantidad,
        prod_rev.subType AS subType
    FROM Process p
    INNER JOIN ProcessRevision pr ON pr.masterRef = p.id_Table
    INNER JOIN ProcessOccurrence po ON po.instancedRef = pr.id_Table
    INNER JOIN ProcessOccurrence po_op ON po_op.parentRef = po.id_Table
    LEFT JOIN OperationRevision op_rev ON op_rev.id_Table = po_op.instancedRef
    LEFT JOIN Operation op ON op.id_Table = op_rev.masterRef
    LEFT JOIN Occurrence o ON o.parentRef = po_op.id_Table
       AND o.subType NOT IN ('MEWorkArea', 'METool')
    LEFT JOIN ProductRevision prod_rev ON prod_rev.id_Table = o.instancedRef
    LEFT JOIN Product prod ON prod.id_Table = prod_rev.masterRef
    CROSS JOIN FirstProcessName fpn
    WHERE prod.productId IS NOT NULL
    GROUP BY fpn.first_process_name, p.catalogueId, prod.productId, prod_rev.subType
    ORDER BY PR_Codigo DESC, Codigo;";

            var converter = new SqlToJsonConverter(connectionString);
            XmlDocument xmlDoc = new XmlDocument();

            string carpetaInput = @"E:\a\Rodrigo Bertero\BOP_input";
            string carpetaProcesados = @"E:\a\Rodrigo Bertero\ProcesadosAgrometalBOP";

            if (!Directory.Exists(carpetaInput))
            {
                Console.WriteLine("No existe carpeta BOP_input");
                return;
            }

            string[] archivos = Directory.GetFiles(carpetaInput);
            int contadorXmls = 1;
            bool borrarTablas = true;

            foreach (string archivo in archivos)
            {
                try
                {
                    Console.WriteLine($"[INFO] Procesando BOP: {Path.GetFileName(archivo)}");
                    xmlDoc.Load(archivo);

                    using (SqlConnection connection = new SqlConnection(connectionString))
                    {
                        connection.Open();
                        XmlNode root = xmlDoc.DocumentElement;
                        var groupedDataRows = new Dictionary<string, List<DataRow>>();

                        if (ParseNode(root, groupedDataRows))
                        {
                            if (borrarTablas)
                            {
                                BorrarTabla(connection, groupedDataRows);
                                borrarTablas = false;
                            }

                            CreateTable(connection, groupedDataRows);
                            InsertData(connection, groupedDataRows, archivo, contadorXmls);

                            // SB1 (productos)
                            foreach (string s in Tablas_SG2_SH3.jsonSB1_BOP()) { 
                                await Tabla_SB1.postSB1(s);
                                Console.WriteLine("-- sb1 --");
                                Console.WriteLine(s);
                            }
                            // SG1 (estructuras)
                            var estructuras = Tabla_SG1.jsonSG1();
                            await Tabla_SG1.postSG1(estructuras);
                            Console.WriteLine("-- sg1 --");
                            Console.WriteLine(estructuras);
                            

                            // SG2/SH3 (procesos)
                            var listaSG2 = Tablas_SG2_SH3.jsonSG2_SH3();
                            foreach (string s in listaSG2) { 
                                await Tablas_SG2_SH3.EnviarSG2_SH3(s);
                                Console.WriteLine("-- sg2/sh3 --");
                                Console.WriteLine(s);
                            }
                            // Mostrar la estructura
                            Console.WriteLine("=== ESTRUCTURA JERÁRQUICA ===");
                            converter.ShowBOMTree(sqlQuery);

                            // JSONs jerárquicos
                            Console.WriteLine("\n=== JSONs INDIVIDUALES ===");
                            converter.ProcessHierarchicalJsons(sqlQuery);

                            var jsonStrings = converter.ConvertToHierarchicalJsonStrings(sqlQuery);
                            Console.WriteLine($"Se generaron {jsonStrings.Count} JSONs jerárquicos");

                            for (int i = 0; i < jsonStrings.Count; i++)
                            {
                                Console.WriteLine($"\nJSON #{i + 1}:");
                                Console.WriteLine(jsonStrings[i]);
                            }

                            // Mover procesado
                            string destino = Path.Combine(carpetaProcesados, Path.GetFileName(archivo));
                            if (File.Exists(destino))
                            {
                                string baseName = Path.GetFileNameWithoutExtension(destino);
                                string ext = Path.GetExtension(destino);
                                destino = Path.Combine(carpetaProcesados, $"{baseName}_{DateTime.Now:yyyyMMdd_HHmmss}{ext}");
                            }
                            File.Move(archivo, destino);
                            contadorXmls++;
                        }
                    }

                    Utilidades.EscribirEnLog($"BOP Procesado: {Path.GetFileName(archivo)}");
                }
                catch (Exception ea)
                {
                    Utilidades.EscribirEnLog($"Error BOP: {Path.GetFileName(archivo)}\nError: {ea.Message}");
                }
            }
        }

        private static async Task ProcesarMBOM(string connectionString)
        {
            Console.WriteLine("=== INICIANDO PROCESAMIENTO MBOM ===");

            XmlDocument xmlDoc = new XmlDocument();

            string carpetaInput = @"E:\a\Rodrigo Bertero\MBOM_input";
            string carpetaProcesados = @"E:\a\Rodrigo Bertero\ProcesadosAgrometalMBOM";

            if (!Directory.Exists(carpetaInput))
            {
                Console.WriteLine("No existe carpeta MBOM_input");
                return;
            }

            string[] archivos = Directory.GetFiles(carpetaInput);
            int contadorXmls = 1;
            bool borrarTablas = true;

            foreach (string archivo in archivos)
            {
                try
                {
                    Console.WriteLine($"[INFO] Procesando MBOM: {Path.GetFileName(archivo)}");
                    xmlDoc.Load(archivo);

                    using (SqlConnection connection = new SqlConnection(connectionString))
                    {
                        connection.Open();
                        XmlNode root = xmlDoc.DocumentElement;
                        var groupedDataRows = new Dictionary<string, List<DataRow>>();

                        if (ParseNode(root, groupedDataRows))
                        {
                            if (borrarTablas)
                            {
                                BorrarTabla(connection, groupedDataRows);
                                borrarTablas = false;
                            }

                            CreateTable(connection, groupedDataRows);
                            InsertData(connection, groupedDataRows, archivo, contadorXmls);

                            // TODO: lógica de generación JSON para MBOM
                            // TODO: integración a Protheus si aplica
                            // SB1 (productos)
                            foreach (string s in Tablas_SG2_SH3.jsonSB1_BOP())
                                await Tabla_SB1.postSB1(s);

                            // SG1 (estructuras)
                            var estructuras = Tabla_SG1.jsonSG1();
                            await Tabla_SG1.postSG1(estructuras);


                            // Mover procesado
                            string destino = Path.Combine(carpetaProcesados, Path.GetFileName(archivo));
                            if (File.Exists(destino))
                            {
                                string baseName = Path.GetFileNameWithoutExtension(destino);
                                string ext = Path.GetExtension(destino);
                                destino = Path.Combine(carpetaProcesados, $"{baseName}_{DateTime.Now:yyyyMMdd_HHmmss}{ext}");
                            }
                            File.Move(archivo, destino);
                            contadorXmls++;
                        }
                    }

                    Utilidades.EscribirEnLog($"MBOM Procesado: {Path.GetFileName(archivo)}");
                }
                catch (Exception ea)
                {
                    Utilidades.EscribirEnLog($"Error MBOM: {Path.GetFileName(archivo)}\nError: {ea.Message}");
                }
            }
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

        //static Dictionary<string, string> Workarea(string connectionString) //Esto obtiene el workarea del proceso
        //{
        //    string sqlworkarea = @"SELECT
	       //                         guest.Process.catalogueId AS instancedProcess,
	       //                         guest.WorkArea.catalogueId AS instancedWorkArea        
	       //                         FROM
		      //                          guest.ProcessOccurrence
	       //                         CROSS APPLY (
		      //                          SELECT TRIM(value) AS FormID, ProcessOccurrence.id_Table
		      //                          FROM STRING_SPLIT(ProcessOccurrence.occurrenceRefs, ' ')
		      //                          WHERE value <> ''
	       //                         ) AS squary1
	       //                         JOIN guest.Occurrence AS occ ON TRY_CAST(SUBSTRING(squary1.FormID, 3, LEN(squary1.FormID)) AS INT) = Occ.parentRef and occ.idXml = ProcessOccurrence.idXml
	       //                         JOIN guest.ProcessRevision ON ProcessRevision.id_Table = ProcessOccurrence.instancedRef and ProcessRevision.idXml = ProcessOccurrence.idXml
	       //                         JOIN guest.Process ON ProcessRevision.masterRef = Process.id_Table and ProcessRevision.idXml = Process.idXml
	       //                         JOIN guest.WorkAreaRevision ON WorkAreaRevision.id_Table = occ.instancedRef and WorkAreaRevision.idXml = occ.idXml
	       //                         JOIN guest.WorkArea ON WorkAreaRevision.masterRef = WorkArea.id_Table and WorkAreaRevision.idXml = WorkArea.idXml
	       //                         WHERE
	       //                         occ.subType = 'MEWorkArea'
	       //                         AND ProcessOccurrence.id_Table = squary1.id_Table";

        //    Dictionary<string, string> resultDictionary = new Dictionary<string, string>();

        //    using (SqlConnection connection = new SqlConnection(connectionString))
        //    {
        //        try
        //        {
        //            connection.Open();

        //            SqlCommand command = new SqlCommand(sqlworkarea, connection);
        //            SqlDataReader reader = command.ExecuteReader();

        //            while (reader.Read())
        //            {
        //                // Leer valores de las columnas
        //                string instancedProcess = reader.GetString(0);
        //                string instancedWorkArea = reader.GetString(1);

        //                // Agregar al diccionario
        //                resultDictionary[instancedProcess] = instancedWorkArea;
        //            }

        //            reader.Close();
        //        }
        //        catch (Exception ex)
        //        {
        //            Utilidades.EscribirEnLog("Error en la consulta de WorkArea: " + ex.Message);
        //        }
        //    }

        //    return resultDictionary;

        //}
        //static List<Dictionary<string, string>> MbomBop(string connectionString) //Relación MBOM-BOP con General Relation
        //{
        //    string sqlRelated = @"select distinct squary1.FormID, CAST(squary1.id_Table as INT),GeneralRelation.relatedRefs,case when ProductRevision.id_Table is not null then Product.productId
		      //                          when guest.ProductRevision.id_Table is null then Process.catalogueId end , GeneralRelation.idXml                                                                
		      //                          from
		      //                          guest.GeneralRelation
		      //                          cross apply (SELECT TRIM(value) AS FormID, GeneralRelation.id_Table
		      //                          FROM STRING_SPLIT(GeneralRelation.relatedRefs, ' ') WHERE value <> '') AS squary1
		      //                          LEFT JOIN guest.ProductRevision ON TRY_CAST(SUBSTRING(squary1.FormID, 4, LEN(squary1.FormID)) AS INT) = ProductRevision.id_Table and GeneralRelation.idXml = ProductRevision.idXml
		      //                          LEFT JOIN guest.ProcessRevision ON TRY_CAST(SUBSTRING(squary1.FormID, 4, LEN(squary1.FormID)) AS INT) = ProcessRevision.id_Table and GeneralRelation.idXml = ProcessRevision.idXml

		      //                          left join guest.Product on Product.id_Table = ProductRevision.masterRef and Product.idXml = ProductRevision.idXml
		      //                          left join guest.Process on Process.id_Table = ProcessRevision.masterRef and Process.idXml = ProcessRevision.idXml

		      //                          where GeneralRelation.id_Table = squary1.id_Table
		      //                          order by GeneralRelation.idXml ASC";
        //    List<Dictionary<string, string>> listRelatedMbomBop = new List<Dictionary<string, string>>();

        //    using (SqlConnection connection = new SqlConnection(connectionString))
        //    {

        //        connection.Open();
        //        SqlCommand command3 = new SqlCommand(sqlRelated, connection);
        //        SqlDataReader reader3 = command3.ExecuteReader();

        //        try
        //        {
        //            int idtableAux = 0;
        //            int idXmlAux = 0;
        //            string codigoAux = string.Empty;

        //            while (reader3.Read())
        //            {
        //                if (reader3.GetInt32(1) == idtableAux && reader3.GetInt32(4) == idXmlAux)
        //                {
        //                    string valCodigo = reader3.IsDBNull(3) ? string.Empty : reader3.GetString(3);

        //                    Dictionary<string, string> diccionario1 = new Dictionary<string, string>
        //                                        {
        //                                            { valCodigo, codigoAux }
        //                                        };
        //                    listRelatedMbomBop.Add(diccionario1);

        //                    codigoAux = string.Empty;
        //                }
        //                else
        //                {
        //                    codigoAux = reader3.IsDBNull(3) ? string.Empty : reader3.GetString(3);
        //                }

        //                idtableAux = reader3.GetInt32(1);
        //                idXmlAux = reader3.GetInt32(4);
        //            }
        //        }
        //        catch (Exception ea)
        //        {
        //            Utilidades.EscribirEnLog("Error en la consulta, related MBOM - BOP: " + ea.Message);
        //        }
        //        finally
        //        {
        //            command3.Cancel();
        //            reader3.Close();
        //            command3.Dispose();
        //        }
        //    }

        //    return listRelatedMbomBop;
        //}
        //static void LimpiarCarpeta(string carpeta) //Limpia la carpeta donde se exportan todos los Xmls, uno por uno antes de volver a exportar más de otra estructura
        //{
        //    try
        //    {


        //        if (Directory.Exists(carpeta))
        //        {
        //            // Borra todos los archivos en la carpeta
        //            string[] archivos = Directory.GetFiles(carpeta);
        //            foreach (string archivo in archivos)
        //            {
        //                File.Delete(archivo);
        //            }

        //            Utilidades.EscribirEnLog($"Carpeta {carpeta} limpiada correctamente.");
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        // Maneja cualquier excepción que pueda ocurrir durante la limpieza
        //        Utilidades.EscribirEnLog($"Error al limpiar la carpeta: {ex.Message}");
        //    }
        //}  

        // --------------------------------------------- Metodos utilizados por el Main() Carga en Base de datos de BOP ---------------------------------------------
        static void BorrarTabla(SqlConnection connection, Dictionary<string, List<DataRow>> groupedDataRows)
        {
            foreach (var group in groupedDataRows)
            {
                try
                {
                    string sql = @"
                                    DECLARE @sql NVARCHAR(MAX) = N'';

                                    SELECT @sql = STRING_AGG(
                                            'DROP TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name),
                                            '; '
                                        )
                                    FROM sys.tables t
                                    JOIN sys.schemas s ON s.schema_id = t.schema_id
                                    WHERE s.name = 'dbo';  -- Cambiar si usás otro esquema

                                    IF @sql IS NOT NULL AND @sql <> ''
                                        EXEC (@sql);";

                    using (var cmd = new SqlCommand(sql, connection))
                    {
                        cmd.ExecuteNonQuery();
                    }

                    Utilidades.EscribirEnLog("✔ Todas las tablas del esquema dbo fueron eliminadas correctamente.");
                }
                catch (Exception ex)
                {
                    Utilidades.EscribirEnLog($"❌ Error al intentar eliminar tablas: {ex.Message}");
                }
           
            }
        }

        static bool ParseNode(XmlNode node, Dictionary<string, List<DataRow>> groupedDataRows, string parentNodeName = "")
        {
            // Crear una lista de nombres de nodos a ignorar
            var listaIgnorados = new List<string> { "ApplicationRef", "AssociatedDataSet", "AttributeContext", "DataSet",
                                                        "ExternalFile", "Folder", "ProductDef",
                                                         "RevisionRule", "Site", "Transform", "View" };
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