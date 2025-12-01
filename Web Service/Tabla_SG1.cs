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
using System.Diagnostics;

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

        // Productos que en la query original venían sin Codigo_Padre
        private static readonly HashSet<string> ProductosSinCodigoPadre =
            new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        public Tabla_SG1()
        {
            _httpClient = new HttpClient();
        }
        public static async Task postSG1(Dictionary<string, List<List<Dictionary<string, string>>>> estructuras)
        {
            string url = "http://119.8.73.193:8086/rest/TCEstructura/Incluir/";
            string username = "USERREST";
            string password = "restagr";

            var putFallBack = new Dictionary<string, List<List<Dictionary<string, string>>>>();

            Console.WriteLine($"[SG1-POST] Iniciando envío masivo a Totvs. Cantidad de productos: {estructuras.Count}");

            using (HttpClient client = new HttpClient())
            {
                var credentials = Encoding.ASCII.GetBytes($"{username}:{password}");
                client.DefaultRequestHeaders.Authorization =
                    new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(credentials));

                foreach (var parent in estructuras)
                {
                    string producto = parent.Key;
                    int cantItems = parent.Value?.Count ?? 0;

                    Console.WriteLine($"[SG1-POST] Preparando envío para producto {producto} con {cantItems} ítems de estructura...");


                    var jsonBody = new
                    {
                        producto = producto,
                        qtdBase = "1",
                        estructura = parent.Value
                    };

                    string jsonData = JsonConvert.SerializeObject(jsonBody, Formatting.Indented);

                    // LOG: preview del JSON
                    Console.WriteLine($"[SG1-POST] JSON generado para producto {producto} (primeros 800 caracteres):");
                    Console.WriteLine(jsonData.Length > 800 ? jsonData.Substring(0, 800) + "..." : jsonData);

                    // Si quisieras ver TODO el JSON, ya lo tenés arriba, solo ojo con el volumen.
                    Utilidades.EscribirEnLog($"[SG1-POST] JSON COMPLETO para producto {producto}:\n{jsonData}");

                    // Obtener el código desde el propio objeto (no hace falta reparsear, pero lo dejo como lo tenés)
                    JObject obj = JObject.Parse(jsonData);
                    string? codigo = obj["producto"]?.ToString();

                    if (string.IsNullOrEmpty(codigo))
                    {
                        Console.WriteLine("[SG1-POST] ERROR: No se pudo obtener el código del producto, se omite este envío.");
                        continue;
                    }

                    Console.WriteLine($"[SG1-POST] Enviando POST para producto {codigo}...");

                    int statusCode = 0;
                    string responseData = string.Empty;

                    try
                    {
                        var content = new StringContent(jsonData, Encoding.UTF8, "application/json");
                        HttpResponseMessage response = await client.PostAsync(url, content);

                        statusCode = (int)response.StatusCode;
                        responseData = await response.Content.ReadAsStringAsync();

                        if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
                        {
                            Console.WriteLine($"[SG1-POST] POST devolvió 409 (Conflict) para {codigo}. Se acumula para PUT.");
                            putFallBack[codigo] = parent.Value;
                        }
                        else if (response.IsSuccessStatusCode)
                        {
                            Console.WriteLine($"[SG1-POST] POST OK para {codigo}: {statusCode} - {responseData}");
                        }
                        else
                        {
                            Console.WriteLine($"[SG1-POST] POST ERROR para {codigo}: {statusCode} {response.ReasonPhrase}. Contenido: {responseData}");
                        }

                        Console.WriteLine($"[SG1-POST] Respuesta cruda para producto {codigo}: {responseData}, status {statusCode}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[SG1-POST] EXCEPCIÓN al enviar producto {producto}: {ex.Message}");
                    }
                    finally
                    {
                        ActualizarBase(statusCode, responseData, codigo);
                    }
                }
            }

            if (putFallBack.Count > 0)
            {
                Console.WriteLine($"[SG1-POST] Finalizado POST. Hay {putFallBack.Count} productos con 409 → se dispara PUT masivo.");
                await putSG1(putFallBack);
            }
            else
            {
                Console.WriteLine("[SG1-POST] Finalizado POST. No hay 409 para procesar con PUT.");
            }
        }



        public async Task postSG1(string jsonString, Dictionary<string, List<List<Dictionary<string, string>>>>? putAcumulador = null)
        {
            string url = "http://119.8.73.193:8086/rest/TCEstructura/Incluir/";
            string username = "USERREST";
            string password = "restagr";

            bool esAcumuladorPropio = false;
            if (putAcumulador == null)
            {
                putAcumulador = new Dictionary<string, List<List<Dictionary<string, string>>>>();
                esAcumuladorPropio = true;
            }

            Console.WriteLine("[SG1-POST-UNIT] Iniciando postSG1(string).");

            using (HttpClient client = new HttpClient())
            {
                var credentials = Encoding.ASCII.GetBytes($"{username}:{password}");
                client.DefaultRequestHeaders.Authorization =
                    new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(credentials));

                try
                {
                    JObject obj = JObject.Parse(jsonString);
                    string? codigo = obj["producto"]?.ToString();

                    if (string.IsNullOrEmpty(codigo))
                    {
                        Console.WriteLine("[SG1-POST-UNIT] ERROR: No se pudo obtener el código del producto del JSON.");
                        Console.WriteLine($"[SG1-POST-UNIT] JSON recibido:\n{jsonString}");
                        return;
                    }

                    Console.WriteLine($"[SG1-POST-UNIT] Código del producto: {codigo}");
                    Console.WriteLine("[SG1-POST-UNIT] JSON a enviar (primeros 800 caracteres):");
                    Console.WriteLine(jsonString.Length > 800 ? jsonString.Substring(0, 800) + "..." : jsonString);

                    int statusCode = 0;
                    string responseData = string.Empty;

                    try
                    {
                        var content = new StringContent(jsonString, Encoding.UTF8, "application/json");
                        HttpResponseMessage response = await client.PostAsync(url, content);

                        statusCode = (int)response.StatusCode;
                        responseData = await response.Content.ReadAsStringAsync();

                        if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
                        {
                            Console.WriteLine($"[SG1-POST-UNIT] POST devolvió 409 para {codigo}. Se acumula para PUT.");
                            var entrada = ConvertirJsonAEstructura(jsonString);
                            putAcumulador[entrada.codigo] = entrada.estructura;
                        }
                        else if (response.IsSuccessStatusCode)
                        {
                            Console.WriteLine($"[SG1-POST-UNIT] POST OK para {codigo}: {statusCode} - {responseData}");
                        }
                        else
                        {
                            Console.WriteLine($"[SG1-POST-UNIT] POST ERROR para {codigo}: {statusCode} {response.ReasonPhrase}. Contenido: {responseData}");
                        }

                        Console.WriteLine($"[SG1-POST-UNIT] Respuesta para producto {codigo}: {responseData}, status {statusCode}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[SG1-POST-UNIT] EXCEPCIÓN al enviar producto {codigo}: {ex.Message}");
                    }
                    finally
                    {
                        ActualizarBase(statusCode, responseData, codigo);
                    }
                }
                catch (JsonException ex)
                {
                    Console.WriteLine($"[SG1-POST-UNIT] Error al parsear JSON: {ex.Message}");
                    Console.WriteLine($"[SG1-POST-UNIT] JSON recibido:\n{jsonString}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[SG1-POST-UNIT] Error general: {ex.Message}");
                }
            }

            if (esAcumuladorPropio && putAcumulador.Count > 0)
            {
                Console.WriteLine($"[SG1-POST-UNIT] Ejecutando PUT masivo (desde postSG1(string)) para {putAcumulador.Count} productos.");
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
        public static Dictionary<string, List<List<Dictionary<string, string>>>> jsonSG1_BOP_Miercoles()
        {
            string connectionString = @"Data Source=DEPLM-11-PC\SQLEXPRESS;Initial Catalog=AgrometalBop;
                                      Integrated Security=True;TrustServerCertificate=True";


            string query = @"WITH FirstProcessName AS (
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
    ORDER BY PR_Codigo DESC, Codigo;
";



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

        //public static async Task putSG1(Dictionary<string, List<List<Dictionary<string, string>>>> estructuras)
        //{
        //    string url = "http://119.8.73.193:8086/rest/TCEstructura/Modificar/";
        //    string username = "USERREST";
        //    string password = "restagr";

        //    using (HttpClient client = new HttpClient())
        //    {
        //        var credentials = Encoding.ASCII.GetBytes($"{username}:{password}");
        //        client.DefaultRequestHeaders.Authorization =
        //            new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(credentials));


        //        foreach (var parent in estructuras)
        //        {
        //            var jsonBody = new
        //            {
        //                producto = parent.Key,
        //                qtdBase = "1",
        //                estructura = parent.Value
        //            };

        //            string jsonData = JsonConvert.SerializeObject(jsonBody, Formatting.Indented);
        //            JObject obj = JObject.Parse(jsonData);

        //            // Obtener directamente el valor del campo "producto"
        //            string? codigo = obj["producto"]?.ToString();

        //            // Asegurarse de que el código no sea nulo
        //            if (string.IsNullOrEmpty(codigo))
        //            {
        //                // Manejar el caso cuando no se encuentra el código del producto
        //                Console.WriteLine("Error: No se pudo obtener el código del producto");
        //                continue;
        //            }

        //            // Ahora puedes usar el código del producto
        //            Console.WriteLine($"Código del producto: {codigo}");

        //            // Continuar con el resto del procesamiento...


        //            // Imprimir el JSON generado
        //            Console.WriteLine("JSON generado:");
        //            Console.WriteLine(jsonData);
        //            Console.WriteLine(codigo);


        //            int statusCode = 0;
        //            string responseData = string.Empty;

        //            try
        //            {
        //                var content = new StringContent(jsonData, Encoding.UTF8, "application/json");
        //                HttpResponseMessage response = await client.PutAsync(url, content);

        //                // Leer el código de estado
        //                statusCode = (int)response.StatusCode;

        //                // Leer la respuesta como string
        //                responseData = await response.Content.ReadAsStringAsync();

        //                // Verificar si la respuesta fue exitosa (puede lanzar excepción)
        //                response.EnsureSuccessStatusCode();

        //                Console.WriteLine($"Respuesta para producto {parent.Key}: {responseData}, {statusCode}");
        //            }
        //            catch (Exception ex)
        //            {
        //                Console.WriteLine($"Error al enviar producto {parent.Key}: {ex.Message}");
        //            }
        //            finally
        //            {
        //                // Se ejecutará siempre, tanto si hay error como si no
        //                ActualizarBase(statusCode, responseData, codigo);
        //            }
        //        }
        //    }
        //}
        public static async Task putSG1(Dictionary<string, List<List<Dictionary<string, string>>>> estructuras)
        {
            string url = "http://119.8.73.193:8086/rest/TCEstructura/Modificar/";
            string username = "USERREST";
            string password = "restagr";

            Console.WriteLine($"[SG1-PUT] Iniciando PUT masivo. Cantidad de productos: {estructuras.Count}");

            using (HttpClient client = new HttpClient())
            {
                var credentials = Encoding.ASCII.GetBytes($"{username}:{password}");
                client.DefaultRequestHeaders.Authorization =
                    new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(credentials));

                foreach (var parent in estructuras)
                {
                    string producto = parent.Key;
                    int cantItems = parent.Value?.Count ?? 0;

                    Console.WriteLine($"[SG1-PUT] Preparando PUT para producto {producto} con {cantItems} ítems de estructura...");

                    var jsonBody = new
                    {
                        producto = parent.Key,
                        qtdBase = "1",
                        estructura = parent.Value
                    };

                    string jsonData = JsonConvert.SerializeObject(jsonBody, Formatting.Indented);
                    JObject obj = JObject.Parse(jsonData);
                    string? codigo = obj["producto"]?.ToString();

                    if (string.IsNullOrEmpty(codigo))
                    {
                        Console.WriteLine("[SG1-PUT] ERROR: No se pudo obtener el código del producto, se omite este envío.");
                        continue;
                    }

                    Console.WriteLine($"[SG1-PUT] JSON generado para producto {codigo} (primeros 800 caracteres):");
                    Console.WriteLine(jsonData.Length > 800 ? jsonData.Substring(0, 800) + "..." : jsonData);

                    int statusCode = 0;
                    string responseData = string.Empty;

                    try
                    {
                        var content = new StringContent(jsonData, Encoding.UTF8, "application/json");
                        HttpResponseMessage response = await client.PutAsync(url, content);

                        statusCode = (int)response.StatusCode;
                        responseData = await response.Content.ReadAsStringAsync();

                        if (response.IsSuccessStatusCode)
                        {
                            Console.WriteLine($"[SG1-PUT] PUT OK para producto {codigo}: {statusCode} - {responseData}");
                        }
                        else
                        {
                            Console.WriteLine($"[SG1-PUT] PUT ERROR para producto {codigo}: {statusCode} {response.ReasonPhrase}. Contenido: {responseData}");
                        }

                        Console.WriteLine($"[SG1-PUT] Respuesta cruda para producto {codigo}: {responseData}, status {statusCode}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[SG1-PUT] EXCEPCIÓN al enviar producto {producto}: {ex.Message}");
                    }
                    finally
                    {
                        ActualizarBase(statusCode, responseData, codigo);
                    }
                }
            }

            Console.WriteLine("[SG1-PUT] PUT masivo finalizado.");
        }


        public static Dictionary<string, List<List<Dictionary<string, string>>>> jsonSG1_MBOM()
        {
            string connectionString = @"Data Source=DEPLM-11-PC\SQLEXPRESS;Initial Catalog=AgrometalBop;
                            Integrated Security=True;TrustServerCertificate=True";

            Console.WriteLine("[SG1-JSON] Iniciando generación de estructuras SG1 desde SQL...");

            string query = @"
        WITH CTE_Hierarchy AS (
            SELECT DISTINCT
                Occurrence.id_table,
                ProductRevision.name,
                Product.productId AS codigo,
                CAST(Occurrence.parentRef AS INT) AS parentRef,
                ProductRevision.revision,
                ProductRevision.subType,
                Occurrence.idXml
            FROM Occurrence
            LEFT JOIN ProductRevision 
                   ON Occurrence.instancedRef = ProductRevision.id_Table
                  AND Occurrence.idXml       = ProductRevision.idXml
            LEFT JOIN Product 
                   ON ProductRevision.masterRef = Product.id_Table
                  AND ProductRevision.idXml    = Product.idXml
            GROUP BY
                Occurrence.id_table, ProductRevision.name, Product.productId,
                Occurrence.parentRef, ProductRevision.revision,
                ProductRevision.subType, Occurrence.idXml
        )
        SELECT DISTINCT
            COALESCE(Parent.name, '')              AS Nombre_Padre,
            COALESCE(CodFmt.CodigoPadre_Final, '') AS Codigo_Padre,      -- código padre formateado
            Child.name                             AS Nombre_Hijo,
            CodFmt.CodigoHijo_Final                AS Codigo_Hijo,       -- código hijo formateado
            Child.subType                          AS Subtype_Hijo,
            Qty.CantidadFinal                      AS CantidadHijo_Total,
            Child.revision                         AS Revision,

            /* ===== NUEVOS CAMPOS ALINEADOS CON SB1 ===== */
            MIN('PA')                              AS Tipo,              -- mismo valor que SB1
            MIN('01')                              AS Deposito,          -- mismo valor que SB1
            MAX(
                CASE 
                    WHEN uudUnidad.title = 'Agm4_Unidad'     THEN uudUnidad.value
                    WHEN uudUnidad.title = 'Agm4_Kilogramos' THEN uudUnidad.value
                    WHEN uudUnidad.title = 'Agm4_Litros'     THEN uudUnidad.value
                    WHEN uudUnidad.title = 'Agm4_Metros'     THEN uudUnidad.value
                    ELSE 'UN'
                END
            )                                      AS unMedida
            /* =========================================== */

        FROM CTE_Hierarchy Child
        LEFT JOIN CTE_Hierarchy Parent 
               ON Child.parentRef = Parent.id_table

        /* ==== JOIN PARA UNIDAD DE MEDIDA (copiado de SB1, adaptado a Child) ==== */
        LEFT JOIN Form fUnidad
               ON Child.codigo = CASE
                                    WHEN CHARINDEX('/', fUnidad.name) > 0 
                                        THEN LEFT(fUnidad.name, CHARINDEX('/', fUnidad.name) - 1)
                                    ELSE fUnidad.name
                                 END
        LEFT JOIN UserValue_UserData uudUnidad
               ON fUnidad.id_Table + 9 = uudUnidad.id_Father
              AND Child.idXml          = uudUnidad.idXml
        /* ====================================================================== */

        /* ---- CANTIDADES por ocurrencia de padre + código de hijo ---- */
        LEFT JOIN (
            SELECT
                oPadre.id_Table AS ParentOccurrenceId,
                pHijo.productId AS ChildCodigo,
                CASE 
                    WHEN prHijo.subType = 'Agm4_MatPrimaRevision'
                        THEN SUM(TRY_CAST(uvud.value AS DECIMAL(18,6)))
                    ELSE COUNT(DISTINCT oHijo.id_Table)
                END AS Cantidad
            FROM Product pHijo
            INNER JOIN ProductRevision prHijo 
                    ON pHijo.id_Table = prHijo.masterRef
            LEFT JOIN Occurrence oHijo       
                   ON oHijo.instancedRef = prHijo.id_Table
            LEFT JOIN UserValue_UserData uvud 
                   ON uvud.id_Father = oHijo.id_Table + 2 
                  AND uvud.title    = 'Quantity'
            LEFT JOIN Occurrence oPadre 
                   ON oHijo.parentRef = oPadre.id_Table
            GROUP BY oPadre.id_Table, pHijo.productId, prHijo.subType
        ) sq3
            ON sq3.ParentOccurrenceId = Parent.id_table
           AND sq3.ChildCodigo        = Child.codigo

        -- Cantidad final (1 si no hay padre; si hay, toma la calculada)
        OUTER APPLY (
            SELECT CAST(
                CASE 
                    WHEN Parent.id_table IS NULL THEN 1
                    ELSE ISNULL(sq3.Cantidad, 1)
                END
            AS DECIMAL(18,2)) AS CantidadFinal
        ) AS Qty
        OUTER APPLY (
            SELECT
                /* Padre: aplicar reglas SB1 si hay código */
                CASE 
                    WHEN Parent.codigo IS NULL THEN NULL
                    ELSE
                        CASE 
                            WHEN LEFT(Parent.codigo, 2) = 'M-' THEN RIGHT(Parent.codigo, LEN(Parent.codigo) - 2)
                            WHEN LEFT(Parent.codigo, 1) = 'M'  THEN RIGHT(Parent.codigo, LEN(Parent.codigo) - 1)
                            WHEN LEFT(Parent.codigo, 1) = 'E'  THEN RIGHT(Parent.codigo, LEN(Parent.codigo) - 1)
                            WHEN RIGHT(Parent.codigo, 3) = '-FV' THEN LEFT(Parent.codigo, LEN(Parent.codigo) - 3)
                            ELSE Parent.codigo
                        END
                END AS CodigoPadre_SB1,

                /* Hijo: aplicar mismas reglas */
                CASE 
                    WHEN Child.codigo IS NULL THEN NULL
                    ELSE
                        CASE 
                            WHEN LEFT(Child.codigo, 2) = 'M-' THEN RIGHT(Child.codigo, LEN(Child.codigo) - 2)
                            WHEN LEFT(Child.codigo, 1) = 'M'  THEN RIGHT(Child.codigo, LEN(Child.codigo) - 1)
                            WHEN LEFT(Child.codigo, 1) = 'E'  THEN RIGHT(Child.codigo, LEN(Child.codigo) - 1)
                            WHEN RIGHT(Child.codigo, 3) = '-FV' THEN LEFT(Child.codigo, LEN(Child.codigo) - 3)
                            ELSE Child.codigo
                        END
                END AS CodigoHijo_SB1
        ) CodSB1
        /* ====== DESPUÉS DE SB1: regla adicional de 'E' y sufijo numérico ====== */
        OUTER APPLY (
            SELECT
                -- Padre: si existe, trabajamos sobre CodigoPadre_SB1
                CASE 
                    WHEN Parent.codigo IS NULL THEN NULL
                    ELSE
                        CASE 
                            WHEN LEFT(CodSB1.CodigoPadre_SB1, 1) = 'E'
                                THEN SUBSTRING(CodSB1.CodigoPadre_SB1, 2, LEN(CodSB1.CodigoPadre_SB1) - 1)
                            ELSE CodSB1.CodigoPadre_SB1
                        END
                END AS CodigoPadre_SinE,

                -- Hijo:
                --   - Si NO tiene padre (raíz): tomamos directo el código ya normalizado por SB1
                --   - Si tiene padre: además aplicamos la regla de 'E' inicial como antes
                CASE 
                    WHEN Parent.codigo IS NULL THEN CodSB1.CodigoHijo_SB1
                    ELSE
                        CASE 
                            WHEN LEFT(CodSB1.CodigoHijo_SB1, 1) = 'E'
                                THEN SUBSTRING(CodSB1.CodigoHijo_SB1, 2, LEN(CodSB1.CodigoHijo_SB1) - 1)
                            ELSE CodSB1.CodigoHijo_SB1
                        END
                END AS CodigoHijo_SinE
        ) CodSinE
        OUTER APPLY (
            SELECT
                CASE 
                    WHEN CodSinE.CodigoPadre_SinE IS NULL THEN NULL
                    ELSE
                        CASE 
                            WHEN RIGHT(CodSinE.CodigoPadre_SinE, 2) LIKE '-[0-9]'
                                THEN LEFT(CodSinE.CodigoPadre_SinE, LEN(CodSinE.CodigoPadre_SinE) - 2)
                            ELSE CodSinE.CodigoPadre_SinE
                        END
                END AS CodigoPadre_Final,

                CASE 
                    WHEN Parent.codigo IS NULL THEN CodSinE.CodigoHijo_SinE
                    ELSE
                        CASE 
                            WHEN RIGHT(CodSinE.CodigoHijo_SinE, 2) LIKE '-[0-9]'
                                THEN LEFT(CodSinE.CodigoHijo_SinE, LEN(CodSinE.CodigoHijo_SinE) - 2)
                            ELSE CodSinE.CodigoHijo_SinE
                        END
                END AS CodigoHijo_Final
        ) CodFmt

        WHERE
            Child.subType IN (
                'Agm4_ConGeneralRevision',
                'Agm4_MatPrimaRevision',
                'Agm4_PiezaRevision',
                'Agm4_RepCompradoRevision',
                'Agm4_SubConRevision',
                'Agm4_sub_mBOM_ERevision'
            )
        GROUP BY
            Parent.name,
            CodFmt.CodigoPadre_Final,
            Child.name,
            CodFmt.CodigoHijo_Final,
            Qty.CantidadFinal,
            Child.revision,
            Child.subType
        ORDER BY
            Codigo_Padre;
        ";

            var estructuras = new Dictionary<string, List<List<Dictionary<string, string>>>>();

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    Console.WriteLine("[SG1-JSON] Conexión a SQL abierta.");

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 5000;
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            var dataByParent = new Dictionary<string, List<DataModel>>();
                            int filasLeidas = 0;

                            while (reader.Read())
                            {
                                filasLeidas++;

                                string parentName = reader["Nombre_Padre"]?.ToString() ?? string.Empty;
                                string rawParentCod = reader["Codigo_Padre"]?.ToString();
                                string childName = reader["Nombre_Hijo"]?.ToString() ?? string.Empty;
                                string childCodigo = reader["Codigo_Hijo"]?.ToString() ?? string.Empty;
                                string cantidadHijo = reader["CantidadHijo_Total"]?.ToString().Replace(',', '.') ?? string.Empty;

                                // ⚠️ AQUÍ VIENE EL CAMBIO IMPORTANTE
                                string parentCodigo = rawParentCod;
                                bool esFilaRaiz = false;

                                if (string.IsNullOrWhiteSpace(parentCodigo))
                                {
                                    if (!string.IsNullOrWhiteSpace(childCodigo))
                                    {
                                        // Esta fila representa al PADRE SUPREMO:
                                        // usamos el código del hijo como código de padre lógico
                                        parentCodigo = childCodigo;
                                        esFilaRaiz = true;
                                        Console.WriteLine($"[SG1-JSON] Fila raíz detectada → ParentCodigo={parentCodigo}, ChildCodigo={childCodigo}");
                                    }
                                    else
                                    {
                                        // Caso realmente inválido: sin padre ni hijo
                                        Console.WriteLine("[SG1-JSON] WARNING: fila sin Codigo_Padre ni Codigo_Hijo. Se omite.");
                                        continue;
                                    }
                                }

                                var model = new DataModel
                                {
                                    ParentName = parentName,
                                    ParentCodigo = parentCodigo,
                                    ChildName = childName,
                                    ChildCodigo = childCodigo,
                                    CantidadHijo = cantidadHijo,
                                    //Variante = ...
                                };

                                // ✅ SIEMPRE poblar la tabla SG1 local (incluye al padre supremo)
                                poblarBaseSG1(parentName, parentCodigo, childName, childCodigo, cantidadHijo);

                                if (!dataByParent.ContainsKey(model.ParentCodigo))
                                    dataByParent[model.ParentCodigo] = new List<DataModel>();

                                dataByParent[model.ParentCodigo].Add(model);
                            }

                            Console.WriteLine($"[SG1-JSON] SQL procesado. Filas leídas: {filasLeidas}. Padres distintos (lógicos): {dataByParent.Count}");

                            // ====== ARMADO DE ESTRUCTURAS PARA CADA PADRE ======
                            foreach (var parentGroup in dataByParent)
                            {
                                string parentCodigo = parentGroup.Key;
                                List<DataModel> children = parentGroup.Value;

                                if (!estructuras.ContainsKey(parentCodigo))
                                    estructuras[parentCodigo] = new List<List<Dictionary<string, string>>>();

                                var allConditions = new Dictionary<string, List<string>>();

                                // Recolección de variantes (si la usás)
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
                                                    allConditions[condition.Key] = new List<string>();

                                                if (!allConditions[condition.Key].Contains(condition.Value))
                                                    allConditions[condition.Key].Add(condition.Value);
                                            }
                                        }
                                        catch (Exception ex)
                                        {
                                            Console.WriteLine($"Error processing Variante '{child.Variante}': {ex.Message}");
                                        }
                                    }
                                }

                                var configCounter = new Dictionary<string, int>();

                                foreach (var child in children)
                                {
                                    // 🚫 NO generar componente autoreferenciado (producto supremo dentro de su propia estructura)
                                    if (!string.IsNullOrEmpty(child.ChildCodigo) &&
                                        child.ChildCodigo == parentCodigo)
                                    {
                                        Console.WriteLine($"[SG1-JSON] INFO: se omite línea autoreferenciada {child.ChildCodigo} en estructura de {parentCodigo}");
                                        continue;
                                    }

                                    var childStructure = new List<Dictionary<string, string>>
                                    {
                                        new() { { "campo", "codigo"   }, { "valor", child.ChildCodigo } },
                                        new() { { "campo", "cantidad" }, { "valor", child.CantidadHijo } }
                                    };

                                    // Sin variante → solo código + cantidad
                                    if (string.IsNullOrEmpty(child.Variante))
                                    {
                                        estructuras[parentCodigo].Add(childStructure);
                                        continue;
                                    }

                                    Dictionary<string, string> conditions;
                                    try
                                    {
                                        conditions = ExtractAllConditions(child.Variante);
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine($"Error extracting conditions from '{child.Variante}': {ex.Message}");
                                        estructuras[parentCodigo].Add(childStructure);
                                        continue;
                                    }

                                    string grupoOpc = "001";
                                    string prefijoOpcional = null;

                                    // (toda la lógica de SSE/SSH/SSM/FSE/... igual que antes)
                                    if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SOLO SEMILLA") &&
                                        ContainsCondition(conditions, "SEMILLA-", "ELECTRICA"))
                                    {
                                        prefijoOpcional = "SSE";
                                    }
                                    else if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SOLO SEMILLA") &&
                                             ContainsCondition(conditions, "SEMILLA-", "HIDRAULICA"))
                                    {
                                        prefijoOpcional = "SSH";
                                    }
                                    else if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SOLO SEMILLA") &&
                                             ContainsCondition(conditions, "SEMILLA-", "MECANICA"))
                                    {
                                        prefijoOpcional = "SSM";
                                    }
                                    else if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SEMILLA + SIMPLE FERTILIZACION") &&
                                             ContainsCondition(conditions, "FERTILIZACION-SIMPLE", "ELECTRICA"))
                                    {
                                        prefijoOpcional = "FSE";
                                    }
                                    else if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SEMILLA + SIMPLE FERTILIZACION") &&
                                             ContainsCondition(conditions, "FERTILIZACION-SIMPLE", "HIDRAULICA"))
                                    {
                                        prefijoOpcional = "FSH";
                                    }
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

                                    if (grupoOpc != null &&
                                        !string.IsNullOrEmpty(prefijoOpcional) &&
                                        prefijoOpcional.Length > 1)
                                    {
                                        if (!configCounter.ContainsKey(prefijoOpcional))
                                            configCounter[prefijoOpcional] = 1;

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

                                    estructuras[parentCodigo].Add(childStructure);
                                }

                                Console.WriteLine($"[SG1-JSON] Padre {parentCodigo}: hijos en estructura = {estructuras[parentCodigo].Count}");
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[SG1-JSON] ERROR al consultar la base: {ex.Message}");
                Console.WriteLine($"[SG1-JSON] Stack trace: {ex.StackTrace}");
            }

            Console.WriteLine($"[SG1-JSON] Estructuras generadas para {estructuras.Count} productos.");

            return estructuras;
        }
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
        public static Dictionary<string, List<List<Dictionary<string, string>>>> jsonSG1_BOP()
        {
            string connectionString = @"Data Source=DEPLM-11-PC\SQLEXPRESS;Initial Catalog=AgrometalBop;
                    Integrated Security=True;TrustServerCertificate=True";

            Console.WriteLine("[SG1-JSON] Iniciando generación de estructuras SG1 desde SQL...");

            string query = @"
WITH CTE_Hierarchy AS (
    SELECT 
        o.id_Table,
        pr.name,
        CASE WHEN LEFT(p.catalogueId, 2) = 'P-' 
             THEN RIGHT(p.catalogueId, LEN(p.catalogueId) - 2) 
             ELSE p.catalogueId 
        END AS catalogueId,
        CAST(o.parentRef AS INT) AS parentRef,
        pr.revision,
        pr.subType,
        o.idXml,

        -- 👇 columnas alineadas con SB1
        'PA' AS Tipo,
        '01' AS Deposito,
        CASE 
            WHEN uudUnidad.title = 'Agm4_Unidad'     THEN uudUnidad.value
            WHEN uudUnidad.title = 'Agm4_Kilogramos' THEN uudUnidad.value
            WHEN uudUnidad.title = 'Agm4_Litros'     THEN uudUnidad.value
            WHEN uudUnidad.title = 'Agm4_Metros'     THEN uudUnidad.value
            ELSE 'UN'
        END AS unMedida

    FROM ProcessOccurrence o
    INNER JOIN ProcessRevision pr ON o.instancedRef = pr.id_Table
    INNER JOIN Process p          ON pr.masterRef   = p.id_Table
    LEFT JOIN Form fUnidad
           ON p.catalogueId = CASE
                                WHEN CHARINDEX('/', fUnidad.name) > 0 
                                     THEN LEFT(fUnidad.name, CHARINDEX('/', fUnidad.name) - 1)
                                ELSE fUnidad.name
                             END
    LEFT JOIN UserValue_UserData uudUnidad
           ON fUnidad.id_Table + 9 = uudUnidad.id_Father
          AND p.idXml              = uudUnidad.idXml
),

CTE_Niveles AS (
    -- Nivel 0 → nodos raíz
    SELECT 
        h.*,
        0 AS Nivel
    FROM CTE_Hierarchy h
    WHERE h.parentRef IS NULL OR h.parentRef = 0

    UNION ALL

    -- Niveles siguientes (hijos)
    SELECT 
        h.*,
        n.Nivel + 1
    FROM CTE_Hierarchy h
    INNER JOIN CTE_Niveles n ON h.parentRef = n.id_Table
)

-- 🟢 PRIMERA PARTE: jerarquía
SELECT
    COALESCE(Parent.name, '')        AS Nombre_Padre,
    COALESCE(Parent.catalogueId, '') AS Codigo_Padre,
    Child.name                       AS Nombre_Hijo,
    Child.catalogueId                AS Codigo_Hijo,
    Child.subType                    AS Subtype_Hijo,
    Child.revision                   AS Revision,
    1                                AS CantidadHijo_Total,
    nChild.Nivel                     AS Nivel,
    Child.Tipo,
    Child.Deposito,
    Child.unMedida
FROM CTE_Niveles nChild
LEFT JOIN CTE_Niveles   nParent ON nChild.parentRef = nParent.id_Table
LEFT JOIN CTE_Hierarchy Parent  ON nParent.id_Table = Parent.id_Table
LEFT JOIN CTE_Hierarchy Child   ON nChild.id_Table  = Child.id_Table

UNION ALL

-- 🟠 SEGUNDA PARTE: MEConsumed (hojas) con la MISMA lógica de unidad
SELECT 
    Operation.name                      AS Nombre_Padre,
    p2.catalogueId                      AS Codigo_Padre,
    p.name                              AS Nombre_Hijo,
    CASE WHEN LEFT(productId, 1) = 'E' 
         THEN RIGHT(productId, LEN(productId) - 1) 
         ELSE productId 
    END                                 AS Codigo_Hijo,
    pr.subType                          AS Subtype_Hijo,
    pr.revision                         AS Revision,
    COUNT(productId)                    AS CantidadHijo_Total,
    3                                   AS Nivel,
    'PA'                                AS Tipo,
    '01'                                AS Deposito,
    -- 👇 lógica de unidad de medida aplicada también acá
    MAX(
        CASE 
            WHEN uudUnidad2.title = 'Agm4_Unidad'     THEN uudUnidad2.value
            WHEN uudUnidad2.title = 'Agm4_Kilogramos' THEN uudUnidad2.value
            WHEN uudUnidad2.title = 'Agm4_Litros'     THEN uudUnidad2.value
            WHEN uudUnidad2.title = 'Agm4_Metros'     THEN uudUnidad2.value
            ELSE 'UN'
        END
    )                                   AS unMedida

FROM Occurrence
INNER JOIN ProductRevision pr ON pr.id_Table = Occurrence.instancedRef
INNER JOIN Product         p  ON p.id_Table  = pr.masterRef
LEFT JOIN ProcessOccurrence o   ON Occurrence.parentRef = o.id_Table
LEFT JOIN OperationRevision op  ON op.id_Table         = o.instancedRef
LEFT JOIN Operation             ON Operation.id_Table  = op.masterRef
LEFT JOIN ProcessOccurrence o2  ON o2.id_Table         = o.parentRef
LEFT JOIN ProcessRevision pr2   ON o2.instancedRef     = pr2.id_Table
LEFT JOIN Process p2            ON pr2.masterRef       = p2.id_Table

-- 👇 joins para unidad de medida en la rama MEConsumed
LEFT JOIN Form fUnidad2
       ON p2.catalogueId = CASE
                             WHEN CHARINDEX('/', fUnidad2.name) > 0 
                                  THEN LEFT(fUnidad2.name, CHARINDEX('/', fUnidad2.name) - 1)
                             ELSE fUnidad2.name
                          END
LEFT JOIN UserValue_UserData uudUnidad2
       ON fUnidad2.id_Table + 9 = uudUnidad2.id_Father
      AND p2.idXml              = uudUnidad2.idXml

WHERE Occurrence.subType = 'MEConsumed'
GROUP BY 
    Operation.name,
    p2.catalogueId,
    p.name,
    productId,
    pr.subType,
    pr.revision
--ORDER BY 
    --Nivel,
    --Codigo_Hijo DESC;

";

            var estructuras = new Dictionary<string, List<List<Dictionary<string, string>>>>();

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    Console.WriteLine("[SG1-JSON] Conexión a SQL abierta.");

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 5000;
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            var dataByParent = new Dictionary<string, List<DataModel>>();
                            int filasLeidas = 0;

                            while (reader.Read())
                            {
                                filasLeidas++;

                                string parentName = reader["Nombre_Padre"]?.ToString() ?? string.Empty;
                                string rawParentCod = reader["Codigo_Padre"]?.ToString();
                                string childName = reader["Nombre_Hijo"]?.ToString() ?? string.Empty;
                                string childCodigo = reader["Codigo_Hijo"]?.ToString() ?? string.Empty;
                                string cantidadHijo = reader["CantidadHijo_Total"]?.ToString().Replace(',', '.') ?? string.Empty;

                                // ⚠️ AQUÍ VIENE EL CAMBIO IMPORTANTE
                                string parentCodigo = rawParentCod;
                                bool esFilaRaiz = false;

                                if (string.IsNullOrWhiteSpace(parentCodigo))
                                {
                                    if (!string.IsNullOrWhiteSpace(childCodigo))
                                    {
                                        // Esta fila representa al PADRE SUPREMO:
                                        // usamos el código del hijo como código de padre lógico
                                        parentCodigo = childCodigo;
                                        esFilaRaiz = true;
                                        Console.WriteLine($"[SG1-JSON] Fila raíz detectada → ParentCodigo={parentCodigo}, ChildCodigo={childCodigo}");
                                    }
                                    else
                                    {
                                        // Caso realmente inválido: sin padre ni hijo
                                        Console.WriteLine("[SG1-JSON] WARNING: fila sin Codigo_Padre ni Codigo_Hijo. Se omite.");
                                        continue;
                                    }
                                }

                                var model = new DataModel
                                {
                                    ParentName = parentName,
                                    ParentCodigo = parentCodigo,
                                    ChildName = childName,
                                    ChildCodigo = childCodigo,
                                    CantidadHijo = cantidadHijo,
                                    //Variante = ...
                                };

                                // ✅ SIEMPRE poblar la tabla SG1 local (incluye al padre supremo)
                                poblarBaseSG1(parentName, parentCodigo, childName, childCodigo, cantidadHijo);

                                if (!dataByParent.ContainsKey(model.ParentCodigo))
                                    dataByParent[model.ParentCodigo] = new List<DataModel>();

                                dataByParent[model.ParentCodigo].Add(model);
                            }

                            Console.WriteLine($"[SG1-JSON] SQL procesado. Filas leídas: {filasLeidas}. Padres distintos (lógicos): {dataByParent.Count}");

                            // ====== ARMADO DE ESTRUCTURAS PARA CADA PADRE ======
                            foreach (var parentGroup in dataByParent)
                            {
                                string parentCodigo = parentGroup.Key;
                                List<DataModel> children = parentGroup.Value;

                                if (!estructuras.ContainsKey(parentCodigo))
                                    estructuras[parentCodigo] = new List<List<Dictionary<string, string>>>();

                                var allConditions = new Dictionary<string, List<string>>();

                                // Recolección de variantes (si la usás)
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
                                                    allConditions[condition.Key] = new List<string>();

                                                if (!allConditions[condition.Key].Contains(condition.Value))
                                                    allConditions[condition.Key].Add(condition.Value);
                                            }
                                        }
                                        catch (Exception ex)
                                        {
                                            Console.WriteLine($"Error processing Variante '{child.Variante}': {ex.Message}");
                                        }
                                    }
                                }

                                var configCounter = new Dictionary<string, int>();

                                foreach (var child in children)
                                {
                                    // 🚫 NO generar componente autoreferenciado (producto supremo dentro de su propia estructura)
                                    if (!string.IsNullOrEmpty(child.ChildCodigo) &&
                                        child.ChildCodigo == parentCodigo)
                                    {
                                        Console.WriteLine($"[SG1-JSON] INFO: se omite línea autoreferenciada {child.ChildCodigo} en estructura de {parentCodigo}");
                                        continue;
                                    }

                                    var childStructure = new List<Dictionary<string, string>>
                            {
                                new() { { "campo", "codigo"   }, { "valor", child.ChildCodigo } },
                                new() { { "campo", "cantidad" }, { "valor", child.CantidadHijo } }
                            };

                                    // Sin variante → solo código + cantidad
                                    if (string.IsNullOrEmpty(child.Variante))
                                    {
                                        estructuras[parentCodigo].Add(childStructure);
                                        continue;
                                    }

                                    Dictionary<string, string> conditions;
                                    try
                                    {
                                        conditions = ExtractAllConditions(child.Variante);
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine($"Error extracting conditions from '{child.Variante}': {ex.Message}");
                                        estructuras[parentCodigo].Add(childStructure);
                                        continue;
                                    }

                                    string grupoOpc = "001";
                                    string prefijoOpcional = null;

                                    // (toda la lógica de SSE/SSH/SSM/FSE/... igual que antes)
                                    if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SOLO SEMILLA") &&
                                        ContainsCondition(conditions, "SEMILLA-", "ELECTRICA"))
                                    {
                                        prefijoOpcional = "SSE";
                                    }
                                    else if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SOLO SEMILLA") &&
                                             ContainsCondition(conditions, "SEMILLA-", "HIDRAULICA"))
                                    {
                                        prefijoOpcional = "SSH";
                                    }
                                    else if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SOLO SEMILLA") &&
                                             ContainsCondition(conditions, "SEMILLA-", "MECANICA"))
                                    {
                                        prefijoOpcional = "SSM";
                                    }
                                    else if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SEMILLA + SIMPLE FERTILIZACION") &&
                                             ContainsCondition(conditions, "FERTILIZACION-SIMPLE", "ELECTRICA"))
                                    {
                                        prefijoOpcional = "FSE";
                                    }
                                    else if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SEMILLA + SIMPLE FERTILIZACION") &&
                                             ContainsCondition(conditions, "FERTILIZACION-SIMPLE", "HIDRAULICA"))
                                    {
                                        prefijoOpcional = "FSH";
                                    }
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

                                    if (grupoOpc != null &&
                                        !string.IsNullOrEmpty(prefijoOpcional) &&
                                        prefijoOpcional.Length > 1)
                                    {
                                        if (!configCounter.ContainsKey(prefijoOpcional))
                                            configCounter[prefijoOpcional] = 1;

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

                                    estructuras[parentCodigo].Add(childStructure);
                                }

                                Console.WriteLine($"[SG1-JSON] Padre {parentCodigo}: hijos en estructura = {estructuras[parentCodigo].Count}");
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[SG1-JSON] ERROR al consultar la base: {ex.Message}");
                Console.WriteLine($"[SG1-JSON] Stack trace: {ex.StackTrace}");
            }

            Console.WriteLine($"[SG1-JSON] Estructuras generadas para {estructuras.Count} productos.");

            return estructuras;
        }



        //        public static void poblarBaseSG1(string Nombre_Padre, string Codigo_Padre, string Nombre_Hijo, string Codigo_Hijo, string CantidadHijo)
        //        {
        //            string connectionString = @"Data Source=DEPLM-11-PC\SQLEXPRESS;Initial Catalog=AgrometalBop;
        //                                      Integrated Security=True;TrustServerCertificate=True";
        //            string query = "INSERT INTO SG1 VALUES (@Nombre_Padre, @Codigo_Padre, @Nombre_Hijo, @Codigo_Hijo, @CantidadHijo, NULL, NULL)";
        //            try
        //            {
        //                using (SqlConnection connection = new SqlConnection(connectionString))
        //                {
        //                    connection.Open();

        //                    using (SqlCommand command = new SqlCommand(query, connection))
        //                    {
        //                        command.Parameters.AddWithValue("@Nombre_Padre", Nombre_Padre);
        //                        command.Parameters.AddWithValue("@Codigo_Padre", Codigo_Padre);
        //                        command.Parameters.AddWithValue("@Nombre_Hijo", Nombre_Hijo);
        //                        command.Parameters.AddWithValue("@Codigo_Hijo", Codigo_Hijo);
        //                        command.Parameters.AddWithValue("@CantidadHijo", CantidadHijo);
        //                        command.ExecuteNonQuery();
        //                    }

        //                }
        //            }
        //            catch (Exception ex)
        //            {

        //            }
        //        }

        //        public static void ActualizarBase(int estado, string mensaje, string codigo)
        //        {
        //            string connectionString = @"Data Source=DEPLM-11-PC\SQLEXPRESS;Initial Catalog=AgrometalBop;
        //                                      Integrated Security=True;TrustServerCertificate=True";
        //            string query = @"UPDATE SG1
        //                          SET estado = @estado, mensaje = @mensaje
        //                          WHERE Codigo_Padre = @codigo
        //--AND descripcion = @descripcion 
        //--AND estado BETWEEN 400 AND 409";
        //            try
        //            {
        //                using (SqlConnection connection = new SqlConnection(connectionString))
        //                {
        //                    connection.Open();

        //                    using (SqlCommand command = new SqlCommand(query, connection))
        //                    {
        //                        command.Parameters.AddWithValue("@estado", estado);
        //                        command.Parameters.AddWithValue("@mensaje", mensaje);
        //                        command.Parameters.AddWithValue("@codigo", codigo);
        //                        command.ExecuteNonQuery();
        //                    }

        //                }
        //            }
        //            catch (Exception ex)
        //            {

        //            }
        //        }
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
                Console.WriteLine($"[SG1-DB] ERROR en poblarBaseSG1 para padre {Codigo_Padre}, hijo {Codigo_Hijo}: {ex.Message}");
            }
        }

        public static void ActualizarBase(int estado, string mensaje, string codigo)
        {
            string connectionString = @"Data Source=DEPLM-11-PC\SQLEXPRESS;Initial Catalog=AgrometalBop;
                                Integrated Security=True;TrustServerCertificate=True";
            string query = @"UPDATE SG1
                     SET estado = @estado, mensaje = @mensaje
                     WHERE Codigo_Padre = @codigo";

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.Parameters.AddWithValue("@estado", estado);
                        command.Parameters.AddWithValue("@mensaje", mensaje ?? string.Empty);
                        command.Parameters.AddWithValue("@codigo", codigo);
                        int rows = command.ExecuteNonQuery();
                        Console.WriteLine($"[SG1-DB] ActualizarBase: estado={estado} codigo={codigo} filas_afectadas={rows}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[SG1-DB] ERROR en ActualizarBase para codigo {codigo}: {ex.Message}");
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
