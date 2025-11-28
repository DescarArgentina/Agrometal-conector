using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Web_Service
{
    public class DataModel
    {
        public string ParentName { get; set; }
        public string ParentCodigo { get; set; }
        public string ChildName { get; set; }
        public string ChildCodigo { get; set; }
        public string CantidadHijo { get; set; }
        public string Variante { get; set; }
    }


    public class Tabla_SG1
    {
        private readonly HttpClient _httpClient;
        public Tabla_SG1()
        {
            _httpClient = new HttpClient();
        }
        public static async Task postSG1(Dictionary<string, List<List<Dictionary<string, string>>>> estructuras)
        {
            string url = "http://119.8.73.193:8086/rest/TCEstructura/Incluir/";
            string username = "USERREST";
            string password = "restagr";
            //string username = "ADMIN"; // Usuario proporcionado
            //string password = "Totvs2024##"; // Contraseña proporcionada

            var putFallBack = new Dictionary<string, List<List<Dictionary<string, string>>>>();

            using (HttpClient client = new HttpClient())
            {
                var credentials = Encoding.ASCII.GetBytes($"{username}:{password}");
                client.DefaultRequestHeaders.Authorization =
                    new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(credentials));


                foreach (var parent in estructuras)
                {
                    var jsonBody = new
                    {
                        producto = parent.Key,
                        qtdBase = "1",
                        estructura = parent.Value
                    };

                    string jsonData = JsonConvert.SerializeObject(jsonBody, Formatting.Indented);
                    JObject obj = JObject.Parse(jsonData);

                    // Obtener directamente el valor del campo "producto"
                    string? codigo = obj["producto"]?.ToString();

                    // Asegurarse de que el código no sea nulo
                    if (string.IsNullOrEmpty(codigo))
                    {
                        // Manejar el caso cuando no se encuentra el código del producto
                        Console.WriteLine("Error: No se pudo obtener el código del producto");
                        continue;
                    }

                    // Ahora puedes usar el código del producto
                    Console.WriteLine($"Código del producto: {codigo}");

                    // Continuar con el resto del procesamiento...
                

                    // Imprimir el JSON generado
                    Console.WriteLine("JSON generado:");
                    Console.WriteLine(jsonData);
                    //Console.WriteLine(codigo);


                    int statusCode = 0;
                    string responseData = string.Empty;

                    try
                    {
                        var content = new StringContent(jsonData, Encoding.UTF8, "application/json");
                        HttpResponseMessage response = await client.PostAsync(url, content);

                        // Leer el código de estado
                        statusCode = (int)response.StatusCode;

                        // Leer la respuesta como string
                        responseData = await response.Content.ReadAsStringAsync();

                        // Verificar si la respuesta fue exitosa (puede lanzar excepción)
                        //response.EnsureSuccessStatusCode();

                        if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
                        {
                            putFallBack[codigo] = parent.Value;
                        }
                        else if (response.IsSuccessStatusCode)
                        {
                            Console.WriteLine($"POST OK para {codigo}: {statusCode} - {responseData}");
                        }
                        else
                        {
                            Console.WriteLine($"POST ERROR para {codigo}: {(int)response.StatusCode} {response.ReasonPhrase}. Contenido: {responseData}");
                        }


                            Console.WriteLine($"Respuesta para producto {parent.Key}: {responseData}, {statusCode}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error al enviar producto {parent.Key}: {ex.Message}");
                    }
                    finally
                    {
                        // Se ejecutará siempre, tanto si hay error como si no
                        ActualizarBase(statusCode, responseData, codigo);
                    }
                }
            }

            if (putFallBack.Count > 0)
            {
                Console.WriteLine($"Ejecutando PUT masivo para {putFallBack.Count} productos (fallback de 409).");
                await putSG1(putFallBack);
            }
        }

        public async Task postSG1(string jsonString, Dictionary<string, List<List<Dictionary<string, string>>>>? putAcumulador = null)
        {
            string url = "http://119.8.73.193:8086/rest/TCEstructura/Incluir/";
            string username = "USERREST";
            string password = "restagr";
            //string username = "ADMIN"; // Usuario proporcionado
            //string password = "Totvs2024##"; // Contraseña proporcionada

            bool esAcumuladorPropio = false;
            if (putAcumulador == null)
            {
                putAcumulador = new Dictionary<string, List<List<Dictionary<string, string>>>>();
                esAcumuladorPropio = true;
            }

            using (HttpClient client = new HttpClient())
            {
                var credentials = Encoding.ASCII.GetBytes($"{username}:{password}");
                client.DefaultRequestHeaders.Authorization =
                    new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(credentials));

                try
                {
                    // Validar que el JSON sea válido
                    JObject obj = JObject.Parse(jsonString);

                    // Obtener directamente el valor del campo "producto"
                    string? codigo = obj["producto"]?.ToString();

                    // Asegurarse de que el código no sea nulo
                    if (string.IsNullOrEmpty(codigo))
                    {
                        Console.WriteLine("Error: No se pudo obtener el código del producto del JSON");
                        Console.WriteLine($"JSON recibido: {jsonString}");
                        return;
                    }

                    // Ahora puedes usar el código del producto
                    Console.WriteLine($"Código del producto: {codigo}");

                    // Imprimir el JSON que se enviará (ya viene formateado)
                    Console.WriteLine("JSON a enviar:");
                    Console.WriteLine(jsonString);

                    int statusCode = 0;
                    string responseData = string.Empty;

                    try
                    {
                        var content = new StringContent(jsonString, Encoding.UTF8, "application/json");
                        HttpResponseMessage response = await client.PostAsync(url, content);

                        // Leer el código de estado
                        statusCode = (int)response.StatusCode;

                        // Leer la respuesta como string
                        responseData = await response.Content.ReadAsStringAsync();

                        // Verificar si la respuesta fue exitosa (puede lanzar excepción)
                        //response.EnsureSuccessStatusCode();

                        if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
                        {
                            Console.WriteLine($"POST devolvió 409 para {codigo}. Lo acumulamos para PUT masivo.");
                            // 👉 Convertir este jsonString a entrada de diccionario y acumular
                            var entrada = ConvertirJsonAEstructura(jsonString);
                            putAcumulador[entrada.codigo] = entrada.estructura;
                        }
                        else if (response.IsSuccessStatusCode)
                        {
                            Console.WriteLine($"POST OK para {codigo}: {statusCode} - {responseData}");
                        }
                        else
                        {
                            Console.WriteLine($"POST ERROR para {codigo}: {(int)response.StatusCode} {response.ReasonPhrase}. Contenido: {responseData}");
                        }

                        Console.WriteLine($"Respuesta para producto {codigo}: {responseData}, {statusCode}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error al enviar producto {codigo}: {ex.Message}");
                    }
                    finally
                    {
                        // Se ejecutará siempre, tanto si hay error como si no
                        ActualizarBase(statusCode, responseData, codigo);
                    }
                }
                catch (JsonException ex)
                {
                    Console.WriteLine($"Error al parsear JSON: {ex.Message}");
                    Console.WriteLine($"JSON recibido: {jsonString}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error general: {ex.Message}");
                }
            }

            if (esAcumuladorPropio && putAcumulador.Count > 0)
            {
                Console.WriteLine($"Ejecutando PUT masivo (desde postSG1(string)) para {putAcumulador.Count} productos.");
                await putSG1(putAcumulador);
            }
        }

        private static (string codigo, List<List<Dictionary<string, string>>> estructura)
        ConvertirJsonAEstructura(string jsonString)
        {
            // Esperamos un JSON con campos: producto (string), estructura (array de arrays de objetos {campo, valor})
            var root = JObject.Parse(jsonString);

            string codigo = root["producto"]?.ToString()
                            ?? throw new InvalidOperationException("JSON sin 'producto'.");

            // estructura: JToken → List<List<Dictionary<string, string>>>
            var estructuraToken = root["estructura"]
                                  ?? throw new InvalidOperationException("JSON sin 'estructura'.");

            var estructura = estructuraToken.ToObject<List<List<Dictionary<string, string>>>>()
                            ?? throw new InvalidOperationException("No se pudo mapear 'estructura'.");

            return (codigo, estructura);
        }


        public static async Task putSG1(Dictionary<string, List<List<Dictionary<string, string>>>> estructuras)
        {
            string url = "http://119.8.73.193:8086/rest/TCEstructura/Modificar/";
            string username = "USERREST";
            string password = "restagr";

            using (HttpClient client = new HttpClient())
            {
                var credentials = Encoding.ASCII.GetBytes($"{username}:{password}");
                client.DefaultRequestHeaders.Authorization =
                    new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(credentials));


                foreach (var parent in estructuras)
                {
                    var jsonBody = new
                    {
                        producto = parent.Key,
                        qtdBase = "1",
                        estructura = parent.Value
                    };

                    string jsonData = JsonConvert.SerializeObject(jsonBody, Formatting.Indented);
                    JObject obj = JObject.Parse(jsonData);

                    // Obtener directamente el valor del campo "producto"
                    string? codigo = obj["producto"]?.ToString();

                    // Asegurarse de que el código no sea nulo
                    if (string.IsNullOrEmpty(codigo))
                    {
                        // Manejar el caso cuando no se encuentra el código del producto
                        Console.WriteLine("Error: No se pudo obtener el código del producto");
                        continue;
                    }

                    // Ahora puedes usar el código del producto
                    Console.WriteLine($"Código del producto: {codigo}");

                    // Continuar con el resto del procesamiento...


                    // Imprimir el JSON generado
                    Console.WriteLine("JSON generado:");
                    Console.WriteLine(jsonData);
                    Console.WriteLine(codigo);


                    int statusCode = 0;
                    string responseData = string.Empty;

                    try
                    {
                        var content = new StringContent(jsonData, Encoding.UTF8, "application/json");
                        HttpResponseMessage response = await client.PutAsync(url, content);

                        // Leer el código de estado
                        statusCode = (int)response.StatusCode;

                        // Leer la respuesta como string
                        responseData = await response.Content.ReadAsStringAsync();

                        // Verificar si la respuesta fue exitosa (puede lanzar excepción)
                        response.EnsureSuccessStatusCode();

                        Console.WriteLine($"Respuesta para producto {parent.Key}: {responseData}, {statusCode}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error al enviar producto {parent.Key}: {ex.Message}");
                    }
                    finally
                    {
                        // Se ejecutará siempre, tanto si hay error como si no
                        ActualizarBase(statusCode, responseData, codigo);
                    }
                }
            }
        }

        //public static async Task putSG1(string jsonString)
        //{
        //    string urlPut = "http://119.8.73.193:8086/rest/TCEstructura/Modificar/";
        //    string username = "USERREST";
        //    string password = "restagr";

        //    using (HttpClient client = new HttpClient())
        //    {
        //        var credentials = Encoding.ASCII.GetBytes($"{username}:{password}");
        //        client.DefaultRequestHeaders.Authorization =
        //            new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(credentials));

        //        string codigo = "(desconocido)";
        //        int statusCode = 0;
        //        string responseData = string.Empty;

        //        try
        //        {
        //            var obj = JObject.Parse(jsonString);
        //            codigo = obj["producto"]?.ToString() ?? "(sin producto)";

        //            var content = new StringContent(jsonString, Encoding.UTF8, "application/json");
        //            HttpResponseMessage response = await client.PutAsync(urlPut, content);

        //            statusCode = (int)response.StatusCode;
        //            responseData = await response.Content.ReadAsStringAsync();

        //            if (response.IsSuccessStatusCode)
        //            {
        //                Console.WriteLine($"PUT OK para producto {codigo}: {responseData}, {statusCode}");
        //            }
        //            else
        //            {
        //                Console.WriteLine($"PUT ERROR para producto {codigo}: {response.StatusCode} - {response.ReasonPhrase}. Contenido: {responseData}");
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            Console.WriteLine($"Error en PUT para producto {codigo}: {ex.Message}");
        //        }
        //        finally
        //        {
        //            ActualizarBase(statusCode, responseData, codigo);
        //        }
        //    }
        //}

        public static Dictionary<string, List<List<Dictionary<string, string>>>> jsonSG1()
        {
            string connectionString = @"Data Source=DEPLM-11-PC\SQLEXPRESS;Initial Catalog=AgrometalBop;
                                      Integrated Security=True;TrustServerCertificate=True";
            

            string query = @"WITH ParentItems AS (
                SELECT 
                    Occurrence.id_table,
                    ProductRevision.name,
                    Occurrence.idXml,
                    CASE 
                        WHEN LEFT(Product.productId, 1) = 'M' THEN RIGHT(Product.productId, LEN(Product.productId) - 2)
                        WHEN LEFT(Product.productId, 1) = 'E' THEN RIGHT(Product.productId, LEN(Product.productId)-1)
                        WHEN RIGHT(Product.productId, 3)= '-FV' THEN LEFT(Product.productId, LEN(Product.productId)-3)
                        ELSE Product.productId 
                    END AS codigo,
                    SUM(TRY_CAST(CASE
                        WHEN UserValue_UserData.value = '' THEN '1'
                        ELSE UserValue_UserData.value
                    END AS FLOAT)) AS Cantidad
                FROM
                    Occurrence
                LEFT JOIN ProductRevision ON Occurrence.instancedRef = ProductRevision.id_Table AND ProductRevision.idXml = Occurrence.idXml
                LEFT JOIN Product ON ProductRevision.masterRef = Product.id_Table AND Product.idXml = ProductRevision.idXml
                LEFT JOIN UserValue_UserData ON Occurrence.id_Table + 2 = UserValue_UserData.id_Father 
                    AND UserValue_UserData.title = 'Quantity' AND Occurrence.idXml = UserValue_UserData.idXml
                WHERE Occurrence.parentRef IS NULL
                GROUP BY ProductRevision.name, Product.productId, Occurrence.id_table, ProductRevision.id_Table, Occurrence.idXml
            )
            SELECT 
                p.id_table AS parent_id_table,
                p.name AS Nombre_Padre,
                p.idXml,
                p.codigo AS Codigo_Padre,
                p.Cantidad AS parent_cantidad,
                c.name AS Nombre_Hijo,
                c.codigo AS Codigo_Hijo,
                SUM(c.Cantidad) AS CantidadHijo_Total,
                c.parentRef
            FROM
                ParentItems p
            LEFT JOIN (
                SELECT 
                    o.id_table,
                    pr.name,
                    o.idXml,
                    CASE 
                        WHEN LEFT(pd.productId, 1) = 'M' THEN RIGHT(pd.productId, LEN(pd.productId) - 2)
                        WHEN LEFT(pd.productId, 1) = 'E' THEN RIGHT(pd.productId, LEN(pd.productId)-1)
                        WHEN RIGHT(pd.productId, 3)= '-FV' THEN LEFT(pd.productId, LEN(pd.productId)-3)
                        ELSE pd.productId 
                    END AS codigo,
                    SUM(TRY_CAST(CASE
                        WHEN uv.value = '' THEN '1'
                        ELSE uv.value
                    END AS FLOAT)) AS Cantidad,
                    CAST(o.parentRef AS INT) AS parentRef
                FROM
                    Occurrence o
                LEFT JOIN ProductRevision pr ON o.instancedRef = pr.id_Table AND pr.idXml = o.idXml
                LEFT JOIN Product pd ON pr.masterRef = pd.id_Table AND pd.idXml = pr.idXml
                LEFT JOIN UserValue_UserData uv ON o.id_Table + 2 = uv.id_Father 
                    AND uv.title = 'Quantity' AND o.idXml = uv.idXml
                WHERE o.parentRef IS NOT NULL
                GROUP BY pr.name, pd.productId, o.parentRef, o.id_table, pr.id_Table, o.idXml
            ) c ON p.id_table = c.parentRef AND p.idXml = c.idXml
            WHERE p.codigo <> c.codigo
            GROUP BY 
                p.id_table,
                p.name,
                p.idXml,
                p.codigo,
                p.Cantidad,
                c.name,
                c.codigo,
                c.parentRef
            ORDER BY p.idXml, p.id_table";

           

            Dictionary<string, List<List<Dictionary<string, string>>>> estructuras = new Dictionary<string, List<List<Dictionary<string, string>>>>();
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 5000;
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            Dictionary<string, List<DataModel>> dataByParent = new Dictionary<string, List<DataModel>>();

                            // Primero, agrupamos todos los datos por ParentCodigo para procesarlos juntos
                            while (reader.Read())
                            {
                                

                                string parentName = reader["Nombre_Padre"]?.ToString() ?? string.Empty;
                                string? parentCodigo = reader["Codigo_Padre"]?.ToString();
                                string childName = reader["Nombre_Hijo"]?.ToString() ?? string.Empty;
                                string childCodigo = reader["Codigo_Hijo"]?.ToString() ?? string.Empty;
                                Console.WriteLine("Este es el codigo que va al json: " + childCodigo);
                                string cantidadHijo = reader["CantidadHijo_Total"]?.ToString().Replace(',', '.') ?? string.Empty;

                                if (string.IsNullOrEmpty(parentCodigo))
                                {
                                    Console.WriteLine("WARNING: Skipping record with null or empty ParentCodigo");
                                    continue;  // Skip this record
                                }

                                var model = new DataModel
                                {
                                    ParentName = parentName,
                                    ParentCodigo = parentCodigo,
                                    ChildName = childName,
                                    ChildCodigo = childCodigo,
                                    CantidadHijo = cantidadHijo,
                                    //Variante = reader.IsDBNull(reader.GetOrdinal("Variante")) ?
                                               //string.Empty : reader["Variante"].ToString()
                                };

                                poblarBaseSG1(parentName, parentCodigo, childName, childCodigo, cantidadHijo);
                                if (!dataByParent.ContainsKey(model.ParentCodigo))
                                {
                                    dataByParent[model.ParentCodigo] = new List<DataModel>();
                                }

                                dataByParent[model.ParentCodigo].Add(model);
                            }

                            // Ahora procesamos cada grupo
                            foreach (var parentGroup in dataByParent)
                            {
                                string parentCodigo = parentGroup.Key;
                                List<DataModel> children = parentGroup.Value;

                                // Skip if parentCodigo is null or empty (should not happen at this point, but just in case)
                                if (string.IsNullOrEmpty(parentCodigo))
                                {
                                    Console.WriteLine("WARNING: Skipping group with null or empty ParentCodigo");
                                    continue;
                                }

                                if (!estructuras.ContainsKey(parentCodigo))
                                {
                                    estructuras[parentCodigo] = new List<List<Dictionary<string, string>>>();
                                }

                                // Analizamos las condiciones de todos los hijos
                                var allConditions = new Dictionary<string, List<string>>();

                                foreach (var child in children)
                                {
                                    if (!string.IsNullOrEmpty(child.Variante))
                                    {
                                        try
                                        {
                                            var conditions = ExtractAllConditions(child.Variante);
                                            foreach (var condition in conditions)
                                            {
                                                if (condition.Key == null)
                                                {
                                                    Console.WriteLine($"WARNING: Null key found in conditions for Variante: {child.Variante}");
                                                    continue;
                                                }

                                                if (!allConditions.ContainsKey(condition.Key))
                                                {
                                                    allConditions[condition.Key] = new List<string>();
                                                }

                                                if (!allConditions[condition.Key].Contains(condition.Value))
                                                {
                                                    allConditions[condition.Key].Add(condition.Value);
                                                }
                                            }
                                        }
                                        catch (Exception ex)
                                        {
                                            Console.WriteLine($"Error processing Variante '{child.Variante}': {ex.Message}");
                                        }
                                    }
                                }

                                // Rest of processing with proper null checking...
                                // (Remaining code follows the same pattern - ensuring no null keys are used)

                                // Process each child and add the relevant conditions
                                var configCounter = new Dictionary<string, int>();

                                foreach (var child in children)
                                {
                                    var childStructure = new List<Dictionary<string, string>>
                                    {
                                        new Dictionary<string, string> { { "campo", "codigo" }, { "valor", child.ChildCodigo } },
                                        new Dictionary<string, string> { { "campo", "cantidad" }, { "valor", child.CantidadHijo } }
                                    };

                                    // If variant is empty, no additional fields needed
                                    if (string.IsNullOrEmpty(child.Variante))
                                    {
                                        estructuras[parentCodigo].Add(childStructure);
                                        continue;
                                    }

                                    // Extract conditions of this child
                                    Dictionary<string, string> conditions;
                                    try
                                    {
                                        conditions = ExtractAllConditions(child.Variante);
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine($"Error extracting conditions from '{child.Variante}': {ex.Message}");
                                        // Add child anyway, without additional conditions
                                        estructuras[parentCodigo].Add(childStructure);
                                        continue;
                                    }

                                    // Verificar combinaciones especiales
                                    string grupoOpc = "001";
                                    string prefijoOpcional = null;

                                    // Case 1: SOLO SEMILLA + ELECTRICA
                                    if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SOLO SEMILLA") &&
                                        ContainsCondition(conditions, "SEMILLA-", "ELECTRICA"))
                                    {
                                        prefijoOpcional = "SSE";
                                    }
                                    // Case 2: SOLO SEMILLA + HIDRAULICA
                                    else if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SOLO SEMILLA") &&
                                             ContainsCondition(conditions, "SEMILLA-", "HIDRAULICA"))
                                    {
                                        prefijoOpcional = "SSH";
                                    }
                                    // Case 3: SOLO SEMILLA + MECANICA
                                    else if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SOLO SEMILLA") &&
                                             ContainsCondition(conditions, "SEMILLA-", "MECANICA"))
                                    {
                                        prefijoOpcional = "SSM";
                                    }
                                    // Case 4: FERTILIZACION SIMPLE + ELECTRICA
                                    else if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SEMILLA + SIMPLE FERTILIZACION") &&
                                             ContainsCondition(conditions, "FERTILIZACION-SIMPLE", "ELECTRICA"))
                                    {
                                        prefijoOpcional = "FSE";
                                    }
                                    // Case 5: FERTILIZACION SIMPLE + HIDRAULICA
                                    else if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SEMILLA + SIMPLE FERTILIZACION") &&
                                            ContainsCondition(conditions, "FERTILIZACION-SIMPLE", "HIDRAULICA"))
                                    {
                                        prefijoOpcional = "FSH";
                                    }
                                    // Case 6: FERTILIZACION SIMPLE + MECANICA
                                    else if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SEMILLA + SIMPLE FERTILIZACION") &&
                                            ContainsCondition(conditions, "FERTILIZACION-SIMPLE", "MECANICA"))
                                    {
                                        prefijoOpcional = "FSM";
                                    }
                                    else if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SEMILLA + DOBLE FERTILIZACION") &&
                                            ContainsCondition(conditions, "FERTILIZACION-DOBLE", "HIDRAULICA"))
                                    {
                                        prefijoOpcional = "FDH";
                                    }
                                    else if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SEMILLA + DOBLE FERTILIZACION") &&
                                            ContainsCondition(conditions, "FERTILIZACION-DOBLE", "MECANICA"))
                                    {
                                        prefijoOpcional = "FDM";
                                    }
                                    else
                                    {
                                        prefijoOpcional = "";
                                        Console.WriteLine(string.Join("", conditions.Keys));
                                    }
                                    // Only add grupo_opc and opcional if we have a defined group
                                    if (grupoOpc != null)
                                    {
                                        // Add grupo_opc
                                       

                                        // Initialize counter for this configuration if it doesn't exist
                                        if (prefijoOpcional != null && !configCounter.ContainsKey(prefijoOpcional))
                                        {
                                            configCounter[prefijoOpcional] = 1;
                                        }

                                        // Format depends on if it's a special case or normal
                                        string valorOpcional;
                                        if (prefijoOpcional != null && prefijoOpcional.Length > 1) // Special case (SSE, SSH, FSE)
                                        {
                                            valorOpcional = $"{prefijoOpcional}";
                                            childStructure.Add(new Dictionary<string, string>
                                {
                                    { "campo", "grupo_opc" },
                                    { "valor", grupoOpc }
                                });
                                            childStructure.Add(new Dictionary<string, string>
                                {
                                    { "campo", "opcional" },
                                    { "valor", prefijoOpcional }
                                });
                                        }

                                    }
                                    else // Normal case with letter (A, B, C, etc.)
                                        {
                                            //valorOpcional = $"{prefijoOpcional ?? "A"}";
                                        }

                                        // Add opcional field
                                       
                                    estructuras[parentCodigo].Add(childStructure);
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al consultar la base de datos: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
            }

            // Generate and print JSON output
            foreach (var parent in estructuras)
            {
                var jsonBody = new
                {
                    producto = parent.Key,
                    qtdBase = "1",
                    estructura = parent.Value
                };

                string jsonData = JsonConvert.SerializeObject(jsonBody, Formatting.Indented);

                // Print the generated JSON
                Console.WriteLine("JSON generado:");
                Console.WriteLine(jsonData);
            }

            return estructuras;
        }

        public static void poblarBaseSG1(string Nombre_Padre, string Codigo_Padre, string Nombre_Hijo, string Codigo_Hijo, string CantidadHijo)
        {
            string connectionString = @"Data Source=DEPLM-11-PC\SQLEXPRESS;Initial Catalog=AgrometalBop;
                                      Integrated Security=True;TrustServerCertificate=True";
            string query = "INSERT INTO SG1 VALUES (@Nombre_Padre, @Codigo_Padre, @Nombre_Hijo, @Codigo_Hijo, @CantidadHijo, NULL, NULL)";
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.Parameters.AddWithValue("@Nombre_Padre", Nombre_Padre);
                        command.Parameters.AddWithValue("@Codigo_Padre", Codigo_Padre);
                        command.Parameters.AddWithValue("@Nombre_Hijo", Nombre_Hijo);
                        command.Parameters.AddWithValue("@Codigo_Hijo", Codigo_Hijo);
                        command.Parameters.AddWithValue("@CantidadHijo", CantidadHijo);
                        command.ExecuteNonQuery();
                    }

                }
            }
            catch (Exception ex)
            {

            }
        }

        public static void ActualizarBase(int estado, string mensaje, string codigo)
        {
            string connectionString = @"Data Source=DEPLM-11-PC\SQLEXPRESS;Initial Catalog=AgrometalBop;
                                      Integrated Security=True;TrustServerCertificate=True";
            string query = @"UPDATE SG1
                          SET estado = @estado, mensaje = @mensaje
                          WHERE Codigo_Padre = @codigo
--AND descripcion = @descripcion 
--AND estado BETWEEN 400 AND 409";
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.Parameters.AddWithValue("@estado", estado);
                        command.Parameters.AddWithValue("@mensaje", mensaje);
                        command.Parameters.AddWithValue("@codigo", codigo);
                        command.ExecuteNonQuery();
                    }

                }
            }
            catch (Exception ex)
            {

            }
        }

        private static bool ContainsCondition(Dictionary<string, string> conditions, string key, string value)
        {
            return conditions.ContainsKey(key) && conditions[key] == value;
        }

        private static Dictionary<string, string> ExtractAllConditions(string varianteText)
        {
            var conditions = new Dictionary<string, string>();

            if (string.IsNullOrEmpty(varianteText))
                return conditions;

            // Split by & to get different conditions
            string[] parts = varianteText.Split(new[] { " & " }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var part in parts)
            {
                try
                {
                    // Look for pattern "[Teamcenter]KEY = VALUE" or "[Teamcenter]''KEY'' = ''VALUE''"
                    string pattern = part.Trim();

                    // Extract the part after [Teamcenter]
                    int teamcenterPos = pattern.IndexOf("[Teamcenter]");
                    if (teamcenterPos >= 0)
                    {
                        string keyValuePart = pattern.Substring(teamcenterPos + "[Teamcenter]".Length).Trim();

                        // Handle case of double single quotes
                        if (keyValuePart.Contains("''"))
                        {
                            // Split by equal sign
                            string[] keyValueSplit = keyValuePart.Split('=');
                            if (keyValueSplit.Length == 2)
                            {
                                // Extract key and value, removing double single quotes
                                string keyWithQuotes = keyValueSplit[0].Trim();
                                string valueWithQuotes = keyValueSplit[1].Trim();

                                // Remove double single quotes
                                string key = keyWithQuotes.Replace("''", "").Trim();
                                string value = valueWithQuotes.Replace("''", "").Trim();

                                // Check for null key
                                if (!string.IsNullOrEmpty(key))
                                {
                                    conditions[key] = value ?? string.Empty;
                                }
                                else
                                {
                                    Console.WriteLine($"Warning: Null or empty key found in '{keyValuePart}'");
                                }
                            }
                        }
                        else
                        {
                            // No quotes, look for equal sign
                            string[] keyValue = keyValuePart.Split('=');
                            if (keyValue.Length == 2)
                            {
                                string key = keyValue[0].Trim();
                                string value = keyValue[1].Trim();

                                // Check for null key
                                if (!string.IsNullOrEmpty(key))
                                {
                                    conditions[key] = value ?? string.Empty;
                                }
                                else
                                {
                                    Console.WriteLine($"Warning: Null or empty key found in '{keyValuePart}'");
                                }
                            }
                        }
                    }
                    else
                    {
                        // Try to extract from other formats like [PREFIX]KEY = VALUE
                        Match match = Regex.Match(pattern, @"\[([^\]]+)\]([^=]+)=(.+)");
                        if (match.Success)
                        {
                            string prefix = match.Groups[1].Value.Trim();
                            string key = match.Groups[2].Value.Trim();
                            string value = match.Groups[3].Value.Trim();

                            // Check for null key
                            if (!string.IsNullOrEmpty(key))
                            {
                                conditions[key] = value ?? string.Empty;
                            }
                            else
                            {
                                Console.WriteLine($"Warning: Null or empty key found in '{pattern}'");
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error processing condition part '{part}': {ex.Message}");
                    // Continue with next condition in case of error
                }
            }
            return conditions;
        }
    }
}
