namespace Web_Service
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Text.RegularExpressions;
    using Newtonsoft.Json;
    using System.Globalization;

    public class ProductStructure
    {
        public string producto { get; set; }
        public string qtdBase { get; set; }
        public List<List<Campo>> estructura { get; set; } = new List<List<Campo>>();
    }

    public class Campo
    {
        public string campo { get; set; }
        public string valor { get; set; }
    }

    public class SqlRecord
    {
        public string Process_codigo { get; set; }
        public string PR_Codigo { get; set; }
        public string Codigo { get; set; }
        public double Cantidad { get; set; }
        public string Subtype { get; set; }

        public string WorkAreaId { get; set; }   // wa.catalogueId (000465, etc.)
        public string Nombre_WA { get; set; }    // wa.nombre si lo traés

        public int NumBusqueda { get; set; }
    }

    public class SqlToJsonConverter
    {
        private readonly string connectionString;

        public SqlToJsonConverter(string connectionString)
        {
            this.connectionString = connectionString;
        }
        public List<ProductStructure> ConvertSqlToHierarchicalJsons(string sqlQuery)
        {
            var records = new List<SqlRecord>();

            // 1) Leer la consulta SQL
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand(sqlQuery, conn))
                using (var rdr = cmd.ExecuteReader())
                {
                    while (rdr.Read())
                    {
                        records.Add(new SqlRecord
                        {
                            Process_codigo = rdr["Process_codigo"].ToString(),
                            PR_Codigo = rdr["PR_Codigo"].ToString(),
                            Codigo = rdr["Codigo"].ToString(),
                            Cantidad = Convert.ToDouble(rdr["Cantidad"]),
                            Subtype = rdr["subType"].ToString(),
                            NumBusqueda = int.TryParse(rdr["num_busqueda"]?.ToString(), out var nb) ? nb : 0,
                            WorkAreaId = rdr["WorkArea_CatalogueId"]?.ToString(),
                            Nombre_WA = rdr["WorkArea_Nombre"]?.ToString()
                        });
                    }
                }
            }

            if (!records.Any())
                return new List<ProductStructure>();

            // Helper: detectar WorkArea de Terceros
            bool EsWorkAreaTerceros(SqlRecord r)
            {
                var id = (r.WorkAreaId ?? string.Empty).Trim();
                var nom = (r.Nombre_WA ?? string.Empty).Trim();

                return id == "000465" ||
                       nom.Equals("Terceros", StringComparison.OrdinalIgnoreCase);
            }

            // 2) Root (ej: 022780) desde Process_codigo
            var match = Regex.Match(records[0].Process_codigo ?? string.Empty, @"\d+");
            string rootParent = match.Success ? match.Value : records[0].Process_codigo;

            // 3) Agrupar PRs con info de NumBusqueda y flag de Terceros
            var prGroups = records
                .Where(r => !string.IsNullOrWhiteSpace(r.PR_Codigo))
                .GroupBy(r => r.PR_Codigo)
                .Select(g => new
                {
                    PR = g.Key,
                    NumBusqueda = g.Max(r => r.NumBusqueda),
                    EsTerceros = g.Any(EsWorkAreaTerceros)
                })
                .ToList();

            // Orden híbrido: num_busqueda (si hay) y luego código
            var prMetaOrdenado = prGroups
                .OrderByDescending(x => x.NumBusqueda > 0)
                .ThenByDescending(x => x.NumBusqueda)
                .ThenBy(x => x.PR)
                .ToList();

            var prList = prMetaOrdenado.Select(x => x.PR).ToList();

            var esTercerosPorPR = prMetaOrdenado.ToDictionary(
                x => x.PR,
                x => x.EsTerceros,
                StringComparer.OrdinalIgnoreCase);

            // 4) Diccionario de estructuras
            var map = new Dictionary<string, ProductStructure>(StringComparer.OrdinalIgnoreCase);

            ProductStructure GetOrCreate(string codigo)
            {
                if (string.IsNullOrWhiteSpace(codigo))
                    throw new ArgumentException("codigo no puede ser nulo o vacío", nameof(codigo));

                if (!map.TryGetValue(codigo, out var ps))
                {
                    ps = new ProductStructure
                    {
                        producto = codigo,
                        qtdBase = "1",
                        estructura = new List<List<Campo>>()
                    };
                    map[codigo] = ps;
                }
                return ps;
            }

            // 5) Calcular padre de cada PR
            var parentByPr = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            string lastNonTerceros = null;

            foreach (var meta in prMetaOrdenado)
            {
                string pr = meta.PR;
                bool esTerceros = meta.EsTerceros;
                string parent;

                if (!esTerceros)
                {
                    parent = lastNonTerceros ?? rootParent;
                    lastNonTerceros = pr;
                }
                else
                {
                    if (lastNonTerceros != null &&
                        parentByPr.TryGetValue(lastNonTerceros, out var parentLast))
                    {
                        parent = parentLast;    // hermano del último PR normal
                    }
                    else
                    {
                        parent = rootParent;    // si es el primero y ya es de terceros
                    }
                }

                parentByPr[pr] = parent;
            }

            // 6) Construir lista de hijos por padre
            var childrenByParent = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

            foreach (var meta in prMetaOrdenado)
            {
                string pr = meta.PR;
                string parent = parentByPr[pr];

                if (!childrenByParent.TryGetValue(parent, out var list))
                {
                    list = new List<string>();
                    childrenByParent[parent] = list;
                }
                list.Add(pr);  // ya viene en orden híbrido
            }

            // Crear root
            GetOrCreate(rootParent);

            // 7) Crear esqueleto de PRs respetando orden
            foreach (var kvp in childrenByParent)
            {
                string parent = kvp.Key;
                var hijos = kvp.Value;

                var psParent = GetOrCreate(parent);

                var hijosNormales = hijos
                    .Where(h => !esTercerosPorPR[h])
                    .ToList();

                var hijosTerceros = hijos
                    .Where(h => esTercerosPorPR[h])
                    .ToList();

                // Primero PR normales
                foreach (var child in hijosNormales)
                {
                    psParent.estructura.Add(new List<Campo>
            {
                new Campo { campo = "codigo",   valor = child },
                new Campo { campo = "cantidad", valor = "1" }
            });

                    GetOrCreate(child);
                }

                // Luego PR de Terceros
                foreach (var child in hijosTerceros)
                {
                    psParent.estructura.Add(new List<Campo>
            {
                new Campo { campo = "codigo",   valor = child },
                new Campo { campo = "cantidad", valor = "1" }
            });

                    GetOrCreate(child);
                }
            }

            // 8) Consumibles de cada PR (ignorando PR de Terceros)
            foreach (var rec in records)
            {
                var st = (rec.Subtype ?? string.Empty).ToLowerInvariant();
                if (st.Contains("operation") || st.Contains("meoperation") ||
                    st.Contains("fixture") || st.Contains("tool"))
                {
                    continue;
                }

                if (string.IsNullOrWhiteSpace(rec.PR_Codigo))
                    continue;

                // Si es PR de Terceros, no cuelgo consumibles
                if (esTercerosPorPR.TryGetValue(rec.PR_Codigo, out var esTer) && esTer)
                    continue;

                var codigoHijo = (rec.Codigo ?? string.Empty).Trim();
                if (string.IsNullOrWhiteSpace(codigoHijo))
                    continue;

                if (rec.Cantidad <= 0)
                    continue;

                var psParent = GetOrCreate(rec.PR_Codigo);

                psParent.estructura.Add(new List<Campo>
        {
            new Campo { campo = "codigo",   valor = codigoHijo },
            new Campo { campo = "cantidad", valor = rec.Cantidad.ToString(CultureInfo.InvariantCulture) }
        });
            }

            // 9) NUEVO: agregar PRxxxxxxT como hijo SV de cada PR NO Terceros
            foreach (var pr in prList)
            {
                if (esTercerosPorPR.TryGetValue(pr, out var esTer) && esTer)
                    continue; // no se generan T para Terceros

                var psParent = GetOrCreate(pr);
                string codigoServ = pr + "T";

                // Evitar duplicados si por alguna razón ya estuviera
                bool yaExiste = psParent.estructura.Any(rel =>
                {
                    var c = rel.FirstOrDefault(x => x.campo == "codigo");
                    return c != null &&
                           string.Equals(c.valor, codigoServ, StringComparison.OrdinalIgnoreCase);
                });

                if (!yaExiste)
                {
                    psParent.estructura.Add(new List<Campo>
            {
                new Campo { campo = "codigo",   valor = codigoServ },
                new Campo { campo = "cantidad", valor = "1" }
            });
                }
            }

            // 10) Lista final: root primero, luego PRs en orden
            var result = new List<ProductStructure>();

            if (map.ContainsKey(rootParent))
                result.Add(map[rootParent]);

            foreach (var pr in prList)
            {
                if (map.TryGetValue(pr, out var ps))
                    result.Add(ps);
            }

            return result;
        }






        public List<string> ConvertToHierarchicalJsonStrings(string sqlQuery)
        {
            var products = ConvertSqlToHierarchicalJsons(sqlQuery);
            return products
                .Select(p => JsonConvert.SerializeObject(p, Formatting.Indented))
                .ToList();
        }

        // Método para mostrar la estructura jerárquica
        private void PrintProductStructure(
            string codigo,
            Dictionary<string, ProductStructure> map,
            string indent = "",
            HashSet<string> visited = null)
        {
            visited ??= new HashSet<string>();

            // Evitar ciclos (por si alguna vez aparece)
            if (visited.Contains(codigo))
            {
                Console.WriteLine($"{indent}- {codigo}  (loop detectado)");
                return;
            }

            visited.Add(codigo);

            if (!map.TryGetValue(codigo, out var product))
            {
                Console.WriteLine($"{indent}- {codigo}  (sin estructura)");
                return;
            }

            Console.WriteLine($"{indent}{codigo}");

            // Si no tiene estructura, no seguimos
            if (product.estructura == null || product.estructura.Count == 0)
                return;

            foreach (var relacion in product.estructura)
            {
                // Cada relacion es una lista de Campos
                var childCode = relacion.First(c => c.campo == "codigo").valor;

                Console.WriteLine($"{indent}  +- {childCode}");

                // Si ese hijo también es padre ? imprimimos recursivamente
                if (map.ContainsKey(childCode))
                    PrintProductStructure(childCode, map, indent + "  ¦   ", visited);
            }
        }



        // Método para guardar cada JSON en archivos separados con nombres descriptivos
        public void SaveHierarchicalJsonFiles(string sqlQuery, string basePath = "")
        {
            var products = ConvertSqlToHierarchicalJsons(sqlQuery);

            for (int i = 0; i < products.Count; i++)
            {
                var product = products[i];

                // Extraemos hasta 3 hijos (si tiene)
                var hijos = product.estructura?
                    .Select(rel => rel.First(c => c.campo == "codigo").valor)
                    .Take(3)
                    .ToList() ?? new List<string>();

                string hijosStr;

                if (hijos.Count == 0)
                {
                    hijosStr = "sin_hijos";
                }
                else
                {
                    // Unir hijos usando guiones bajos
                    hijosStr = string.Join("_", hijos);
                }

                // Armar el JSON
                string json = JsonConvert.SerializeObject(product, Formatting.Indented);

                // Construir nombre descriptivo
                string fileName =
                    $"{basePath}relacion_{i + 1:D3}_padre_{product.producto}_hijos_{hijosStr}.json";

                // Guardar archivo
                System.IO.File.WriteAllText(fileName, json);

                Console.WriteLine($"Guardado: {fileName}");
            }
        }



        // Método para procesar y mostrar cada JSON individual
        public void ProcessHierarchicalJsons(string sqlQuery)
        {
            var products = ConvertSqlToHierarchicalJsons(sqlQuery);

            Console.WriteLine($"Total de JSONs jerárquicos generados: {products.Count}");
            Console.WriteLine(new string('=', 50));

            for (int i = 0; i < products.Count; i++)
            {
                var product = products[i];
                string json = JsonConvert.SerializeObject(product, Formatting.Indented);

                Console.WriteLine($"JSON #{i + 1}:");
                Console.WriteLine(json);
                Console.WriteLine(new string('-', 30));
            }
        }

        // Método para generar un resumen de la cadena jerárquica
        public void ShowBOMTree(string sqlQuery)
        {
            Console.WriteLine("ÁRBOL COMPLETO DE LA ESTRUCTURA BOM");
            Console.WriteLine("====================================");

            // Obtenemos la estructura jerárquica
            var products = ConvertSqlToHierarchicalJsons(sqlQuery);

            // Convertimos la lista en un diccionario para acceso rápido
            var map = products.ToDictionary(p => p.producto, p => p);

            // El primero siempre es el root (ej: 22116)
            var root = products[0].producto;

            PrintProductStructure(root, map);
        }

    }
}


