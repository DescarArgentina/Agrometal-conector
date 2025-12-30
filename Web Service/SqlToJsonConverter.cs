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
        public string fecha { get; set; }
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






        //        public List<ProductStructure> ConvertSqlToHierarchicalJsons(string sqlQuery)
        //        {
        //            var records = new List<SqlRecord>();

        //            // Helpers de lectura robusta (DBNull-safe)
        //            string GetString(SqlDataReader r, string col) =>
        //                r.IsDBNull(r.GetOrdinal(col)) ? null : r[col].ToString();

        //            double GetDouble(SqlDataReader r, string col) =>
        //                r.IsDBNull(r.GetOrdinal(col)) ? 0 : Convert.ToDouble(r[col]);

        //            int GetInt(SqlDataReader r, string col) =>
        //                r.IsDBNull(r.GetOrdinal(col)) ? 0 : (int.TryParse(r[col].ToString(), out var v) ? v : 0);

        //            // Helper: extraer número de un código (PR102029 -> 102029)
        //            int ExtraerNumero(string s)
        //            {
        //                var m = Regex.Match(s ?? string.Empty, @"\d+");
        //                return (m.Success && int.TryParse(m.Value, out var n)) ? n : 0;
        //            }

        //            // Helper: emular LEFT(p.catalogueId,3)
        //            string Left3(string s)
        //            {
        //                if (string.IsNullOrWhiteSpace(s)) return string.Empty;
        //                s = s.Trim();
        //                return s.Length >= 3 ? s.Substring(0, 3) : s;
        //            }

        //            // 1) Leer la consulta SQL
        //            using (var conn = new SqlConnection(connectionString))
        //            {
        //                conn.Open();
        //                using (var cmd = new SqlCommand(sqlQuery, conn))
        //                using (var rdr = cmd.ExecuteReader())
        //                {
        //                    while (rdr.Read())
        //                    {
        //                        records.Add(new SqlRecord
        //                        {
        //                            Process_codigo = GetString(rdr, "Process_codigo"),
        //                            PR_Codigo = GetString(rdr, "PR_Codigo"),
        //                            Codigo = GetString(rdr, "Codigo"),
        //                            Cantidad = GetDouble(rdr, "Cantidad"),
        //                            Subtype = GetString(rdr, "subType"),
        //                            NumBusqueda = GetInt(rdr, "num_busqueda"), // si es alfanumérico (ej 481703X) queda 0
        //                            WorkAreaId = GetString(rdr, "WorkArea_CatalogueId"),
        //                            Nombre_WA = GetString(rdr, "WorkArea_Nombre")
        //                        });
        //                    }
        //                }
        //            }

        //            if (!records.Any())
        //                return new List<ProductStructure>();

        //            // Helper: detectar WorkArea de Terceros
        //            bool EsWorkAreaTerceros(SqlRecord r)
        //            {
        //                var id = (r.WorkAreaId ?? string.Empty).Trim();
        //                var nom = (r.Nombre_WA ?? string.Empty).Trim();

        //                return id == "000465" ||
        //                       nom.Equals("Terceros", StringComparison.OrdinalIgnoreCase);
        //            }

        //            // 2) Root (ej: 025900) desde Process_codigo
        //            var match = Regex.Match(records[0].Process_codigo ?? string.Empty, @"\d+");
        //            string rootParent = match.Success ? match.Value : (records[0].Process_codigo ?? string.Empty).Trim();

        //            // 3) Agrupar PRs con info de NumBusqueda y flag de Terceros
        //            var prGroups = records
        //                .Where(r => !string.IsNullOrWhiteSpace(r.PR_Codigo))
        //                .GroupBy(r => r.PR_Codigo.Trim(), StringComparer.OrdinalIgnoreCase)
        //                .Select(g => new
        //                {
        //                    PR = g.Key,
        //                    NumBusqueda = g.Max(r => r.NumBusqueda),
        //                    Prefix3 = Left3(g.Key),          // emula LEFT(p.catalogueId,3)
        //                    PrNum = ExtraerNumero(g.Key),    // para desempate estable
        //                    EsTerceros = g.Any(EsWorkAreaTerceros)
        //                })
        //                .ToList();

        //            // Orden: num_busqueda DESC, luego LEFT(PR,3) DESC, luego número DESC (desempate)
        //            var prMetaOrdenado = prGroups
        //                .OrderByDescending(x => x.NumBusqueda)
        //                .ThenByDescending(x => x.Prefix3, StringComparer.OrdinalIgnoreCase)
        //                .ThenByDescending(x => x.PrNum)
        //                .ThenBy(x => x.PR, StringComparer.OrdinalIgnoreCase)
        //                .ToList();

        //            var prList = prMetaOrdenado.Select(x => x.PR).ToList();

        //            var esTercerosPorPR = prMetaOrdenado.ToDictionary(
        //                x => x.PR,
        //                x => x.EsTerceros,
        //                StringComparer.OrdinalIgnoreCase);

        //            // 4) Diccionario de estructuras
        //            var map = new Dictionary<string, ProductStructure>(StringComparer.OrdinalIgnoreCase);

        //            ProductStructure GetOrCreate(string codigo)
        //            {
        //                if (string.IsNullOrWhiteSpace(codigo))
        //                    throw new ArgumentException("codigo no puede ser nulo o vacío", nameof(codigo));

        //                if (!map.TryGetValue(codigo, out var ps))
        //                {
        //                    ps = new ProductStructure
        //                    {
        //                        producto = codigo,
        //                        qtdBase = "1",
        //                        estructura = new List<List<Campo>>()
        //                    };
        //                    map[codigo] = ps;
        //                }
        //                return ps;
        //            }

        //            // 5) Calcular padre de cada PR
        //            var parentByPr = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        //            string lastNonTerceros = null;

        //            foreach (var meta in prMetaOrdenado)
        //            {
        //                string pr = meta.PR;
        //                bool esTerceros = meta.EsTerceros;
        //                string parent;

        //                if (!esTerceros)
        //                {
        //                    parent = lastNonTerceros ?? rootParent;
        //                    lastNonTerceros = pr;
        //                }
        //                else
        //                {
        //                    if (lastNonTerceros != null &&
        //                        parentByPr.TryGetValue(lastNonTerceros, out var parentLast))
        //                    {
        //                        parent = parentLast; // hermano del último PR normal
        //                    }
        //                    else
        //                    {
        //                        parent = rootParent; // si es el primero y ya es de terceros
        //                    }
        //                }

        //                parentByPr[pr] = parent;
        //            }

        //            // 6) Construir lista de hijos por padre
        //            var childrenByParent = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

        //            foreach (var meta in prMetaOrdenado)
        //            {
        //                string pr = meta.PR;
        //                string parent = parentByPr[pr];

        //                if (!childrenByParent.TryGetValue(parent, out var list))
        //                {
        //                    list = new List<string>();
        //                    childrenByParent[parent] = list;
        //                }
        //                list.Add(pr); // ya viene en el orden correcto
        //            }

        //            // Crear root
        //            GetOrCreate(rootParent);

        //            // 7) Crear esqueleto de PRs respetando orden (normales primero, terceros después)
        //            foreach (var kvp in childrenByParent)
        //            {
        //                string parent = kvp.Key;
        //                var hijos = kvp.Value;

        //                var psParent = GetOrCreate(parent);

        //                var hijosNormales = hijos
        //                    .Where(h => !esTercerosPorPR[h])
        //                    .ToList();

        //                var hijosTerceros = hijos
        //                    .Where(h => esTercerosPorPR[h])
        //                    .ToList();

        //                foreach (var child in hijosNormales)
        //                {
        //                    psParent.estructura.Add(new List<Campo>
        //            {
        //                new Campo { campo = "codigo",   valor = child },
        //                new Campo { campo = "cantidad", valor = "1" }
        //            });

        //                    GetOrCreate(child);
        //                }

        //                foreach (var child in hijosTerceros)
        //                {
        //                    psParent.estructura.Add(new List<Campo>
        //            {
        //                new Campo { campo = "codigo",   valor = child },
        //                new Campo { campo = "cantidad", valor = "1" }
        //            });

        //                    GetOrCreate(child);
        //                }
        //            }

        //            // 8) Agregar PRxxxxxxT como hijo SV de cada PR NO Terceros (ANTES de consumibles)
        //            foreach (var pr in prList)
        //            {
        //                if (esTercerosPorPR.TryGetValue(pr, out var esTer) && esTer)
        //                    continue; // no se generan T para Terceros

        //                var psParent = GetOrCreate(pr);
        //                string codigoServ = pr + "T";

        //                bool yaExiste = psParent.estructura.Any(rel =>
        //                {
        //                    var c = rel.FirstOrDefault(x => x.campo == "codigo");
        //                    return c != null &&
        //                           string.Equals(c.valor, codigoServ, StringComparison.OrdinalIgnoreCase);
        //                });

        //                if (!yaExiste)
        //                {
        //                    psParent.estructura.Add(new List<Campo>
        //            {
        //                new Campo { campo = "codigo",   valor = codigoServ },
        //                new Campo { campo = "cantidad", valor = "1" }
        //            });
        //                }
        //            }

        //            // 9) Consumibles de cada PR (ignorando PR de Terceros) + no informar hijos con Cantidad <= 0
        //            foreach (var rec in records)
        //            {
        //                var st = (rec.Subtype ?? string.Empty).ToLowerInvariant();
        //                if (st.Contains("operation") || st.Contains("meoperation") ||
        //                    st.Contains("fixture") || st.Contains("tool"))
        //                {
        //                    continue;
        //                }

        //                if (string.IsNullOrWhiteSpace(rec.PR_Codigo))
        //                    continue;

        //                // Si es PR de Terceros, no cuelgo consumibles
        //                if (esTercerosPorPR.TryGetValue(rec.PR_Codigo.Trim(), out var esTer) && esTer)
        //                    continue;

        //                var codigoHijo = (rec.Codigo ?? string.Empty).Trim();
        //                if (string.IsNullOrWhiteSpace(codigoHijo))
        //                    continue;

        //                if (rec.Cantidad <= 0)
        //                    continue;

        //                var psParent = GetOrCreate(rec.PR_Codigo.Trim());

        //                psParent.estructura.Add(new List<Campo>
        //        {
        //            new Campo { campo = "codigo",   valor = codigoHijo },
        //            new Campo { campo = "cantidad", valor = rec.Cantidad.ToString(CultureInfo.InvariantCulture) }
        //        });
        //            }

        //            // 10) Lista final: root primero, luego PRs en orden
        //            var result = new List<ProductStructure>();

        //            if (map.ContainsKey(rootParent))
        //                result.Add(map[rootParent]);

        //            foreach (var pr in prList)
        //            {
        //                if (map.TryGetValue(pr, out var ps))
        //                    result.Add(ps);
        //            }

        //            return result;
        //        }



        public List<ProductStructure> ConvertSqlToHierarchicalJsons(string sqlQuery)
        {
            var records = new List<SqlRecord>();

            // Helpers de lectura robusta (DBNull-safe)
            string GetString(SqlDataReader r, string col) =>
                r.IsDBNull(r.GetOrdinal(col)) ? null : r[col].ToString();

            double GetDouble(SqlDataReader r, string col) =>
                r.IsDBNull(r.GetOrdinal(col)) ? 0 : Convert.ToDouble(r[col]);

            int GetInt(SqlDataReader r, string col) =>
                r.IsDBNull(r.GetOrdinal(col)) ? 0 : (int.TryParse(r[col].ToString(), out var v) ? v : 0);

            // Helper: extraer número de un código (PR102029 -> 102029)
            int ExtraerNumero(string s)
            {
                var m = Regex.Match(s ?? string.Empty, @"\d+");
                return (m.Success && int.TryParse(m.Value, out var n)) ? n : 0;
            }

            // Helper: extraer dígitos como string (para comparar con root incluyendo ceros a la izquierda)
            string ExtraerDigitos(string s)
            {
                var m = Regex.Match(s ?? string.Empty, @"\d+");
                return m.Success ? m.Value : string.Empty;
            }

            // Helper: emular LEFT(p.catalogueId,3)
            string Left3(string s)
            {
                if (string.IsNullOrWhiteSpace(s)) return string.Empty;
                s = s.Trim();
                return s.Length >= 3 ? s.Substring(0, 3) : s;
            }

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
                            Process_codigo = GetString(rdr, "Process_codigo"),
                            PR_Codigo = GetString(rdr, "PR_Codigo"),
                            Codigo = GetString(rdr, "Codigo"),
                            Cantidad = GetDouble(rdr, "Cantidad"),
                            Subtype = GetString(rdr, "subType"),
                            NumBusqueda = GetInt(rdr, "num_busqueda"), // si es alfanumérico (ej 481703X) queda 0
                            WorkAreaId = GetString(rdr, "WorkArea_CatalogueId"),
                            Nombre_WA = GetString(rdr, "WorkArea_Nombre")
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
                       nom.IndexOf("TERCEROS", StringComparison.OrdinalIgnoreCase) >= 0;
            }

            // 2) Root (ej: 025900) desde Process_codigo
            var match = Regex.Match(records[0].Process_codigo ?? string.Empty, @"\d+");
            string proc = (records[0].Process_codigo ?? string.Empty).Trim();

            string rootParent;
            if (proc.StartsWith("P-", StringComparison.OrdinalIgnoreCase))
                rootParent = proc.Substring(2).Trim();   // conserva "066650" o "MEGA1270151"
            else
                rootParent = proc;                       // conserva "MEGA1270151"


            // Requerimiento SG1:
            // - No informar como hijo del producto final el PR con MAYOR num_busqueda si NO es de terceros.
            // - En su lugar, informar el servicio como PR + "T" (T al final) como hermano del PR siguiente.
            // - Si el proceso tiene un único PR (y NO es de terceros), se elimina ese PR y el producto final pasa a tener como hijos
            //   la materia prima (consumibles) y el servicio (PR + "T").
            // 3) Agrupar PRs con info de NumBusqueda y flag de Terceros
            var prGroups = records
                .Where(r => !string.IsNullOrWhiteSpace(r.PR_Codigo))
                .GroupBy(r => r.PR_Codigo.Trim(), StringComparer.OrdinalIgnoreCase)
                .Select(g => new
                {
                    PR = g.Key,
                    NumBusqueda = g.Max(r => r.NumBusqueda),
                    Prefix3 = Left3(g.Key),          // emula LEFT(p.catalogueId,3)
                    PrNum = ExtraerNumero(g.Key),    // para desempate estable
                    EsTerceros = g.Any(EsWorkAreaTerceros)
                })
                .ToList();

            // Orden: num_busqueda DESC, luego LEFT(PR,3) DESC, luego número DESC (desempate)
            var prMetaOrdenado = prGroups
                .OrderByDescending(x => x.NumBusqueda)
                .ThenByDescending(x => x.Prefix3, StringComparer.OrdinalIgnoreCase)
                .ThenByDescending(x => x.PrNum)
                .ThenBy(x => x.PR, StringComparer.OrdinalIgnoreCase)
                .ToList();

            // Determinar PR a remover según el requerimiento.
            // Regla 0: si el último PR (menor num_busqueda) es de Terceros => NO aplicar cambios, se mantiene tal como hoy.
            // Regla 1: si hay un único PR y NO es de terceros => se elimina.
            // Regla 2: si hay más de un PR, el PR con MAYOR num_busqueda se elimina solo si NO es de terceros.
            string prARemover = null;

            bool ultimoEsTerceros = prMetaOrdenado.Count > 0 &&
                                   prMetaOrdenado[prMetaOrdenado.Count - 1].EsTerceros;

            if (!ultimoEsTerceros)
            {
                if (prMetaOrdenado.Count == 1 && !prMetaOrdenado[0].EsTerceros)
                {
                    prARemover = prMetaOrdenado[0].PR;
                }
                else if (prMetaOrdenado.Count > 1 && !prMetaOrdenado[0].EsTerceros)
                {
                    prARemover = prMetaOrdenado[0].PR; // mayor num_busqueda
                }
            }

            if (!string.IsNullOrWhiteSpace(prARemover))
            {
                string procesoP = $"P-{rootParent}";
                Sg1Exclusions.SetExcluded(procesoP, prARemover);
            }
            if (!string.IsNullOrWhiteSpace(prARemover))
            {
                string procesoP = $"P-{rootParent}";
                Sg1Exclusions.SetExcluded(procesoP, prARemover);

                Utilidades.EscribirEnLog($"SG1 -> Excluido PR={prARemover} y se mapeará en SG2 al proceso {procesoP}");
            }




            // Lista operativa de PRs (ya sin el PR removido) (ya sin el PR removido)
            var prMetaOperativa = prMetaOrdenado
                .Where(x => !string.Equals(x.PR, prARemover, StringComparison.OrdinalIgnoreCase))
                .ToList();

            var prList = prMetaOperativa.Select(x => x.PR).ToList();

            var esTercerosPorPR = prMetaOrdenado.ToDictionary(
                x => x.PR,
                x => x.EsTerceros,
                StringComparer.OrdinalIgnoreCase);

            // Padre donde "colgar" el PRT y los consumibles del PR removido (hermano del PR siguiente)
            // Por defecto: rootParent.
            // Si existe al menos un PR operativo, usamos el padre del primero (habitualmente rootParent).
            string padrePRRemovido = rootParent;

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

            foreach (var meta in prMetaOperativa)
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
                        parent = parentLast; // hermano del último PR normal
                    }
                    else
                    {
                        parent = rootParent; // si es el primero y ya es de terceros
                    }
                }

                parentByPr[pr] = parent;
            }

            if (prMetaOperativa.Count > 0)
            {
                var firstPr = prMetaOperativa[0].PR;
                if (parentByPr.TryGetValue(firstPr, out var p))
                    padrePRRemovido = p;
            }

            // 6) Construir lista de hijos por padre
            var childrenByParent = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

            foreach (var meta in prMetaOperativa)
            {
                string pr = meta.PR;
                string parent = parentByPr[pr];

                if (!childrenByParent.TryGetValue(parent, out var list))
                {
                    list = new List<string>();
                    childrenByParent[parent] = list;
                }
                list.Add(pr); // ya viene en el orden correcto
            }

            // Crear root
            GetOrCreate(rootParent);

            // 7) Crear esqueleto de PRs respetando orden (normales primero, terceros después)
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

                foreach (var child in hijosNormales)
                {
                    psParent.estructura.Add(new List<Campo>
            {
                new Campo { campo = "codigo",   valor = child },
                new Campo { campo = "cantidad", valor = "1" }
            });

                    GetOrCreate(child);
                }

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

            // 8) Agregar PRxxxxxxT como hijo SV de cada PR NO Terceros (ANTES de consumibles)
            foreach (var pr in prList)
            {
                if (esTercerosPorPR.TryGetValue(pr, out var esTer) && esTer)
                    continue; // no se generan T para Terceros

                var psParent = GetOrCreate(pr);
                string codigoServ = pr + "T";

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

            // 8.b) Si hubo PR removido, agregar su PRT como hijo del padre correspondiente (hermano del PR siguiente)
            if (!string.IsNullOrWhiteSpace(prARemover))
            {
                var psPadre = GetOrCreate(padrePRRemovido);
                string codigoServRemovido = prARemover + "T";

                bool yaExiste = psPadre.estructura.Any(rel =>
                {
                    var c = rel.FirstOrDefault(x => x.campo == "codigo");
                    return c != null &&
                           string.Equals(c.valor, codigoServRemovido, StringComparison.OrdinalIgnoreCase);
                });

                if (!yaExiste)
                {
                    // Insertar el PRT cerca del PR siguiente (mismo nivel). Si no existe PR siguiente, se agrega al final.
                    int insertAt = psPadre.estructura.Count;
                    if (prList.Count > 0)
                    {
                        string prSiguiente = prList[0];
                        int idx = psPadre.estructura.FindIndex(rel =>
                        {
                            var c = rel.FirstOrDefault(x => x.campo == "codigo");
                            return c != null && string.Equals(c.valor, prSiguiente, StringComparison.OrdinalIgnoreCase);
                        });
                        if (idx >= 0)
                            insertAt = idx + 1;
                    }

                    psPadre.estructura.Insert(insertAt, new List<Campo>
                    {
                        new Campo { campo = "codigo",   valor = codigoServRemovido },
                        new Campo { campo = "cantidad", valor = "1" }
                    });
                }
            }

            // 9) Consumibles de cada PR (ignorando PR de Terceros) + no informar hijos con Cantidad <= 0
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

                // Si el PR es el removido, colgamos sus consumibles del padre correspondiente (producto final)
                var prRec = rec.PR_Codigo.Trim();
                bool prEsRemovido = !string.IsNullOrWhiteSpace(prARemover) &&
                                   string.Equals(prRec, prARemover, StringComparison.OrdinalIgnoreCase);

                // Si es PR de Terceros, no cuelgo consumibles
                if (!prEsRemovido && esTercerosPorPR.TryGetValue(prRec, out var esTer) && esTer)
                    continue;

                var codigoHijo = (rec.Codigo ?? string.Empty).Trim();
                if (string.IsNullOrWhiteSpace(codigoHijo))
                    continue;

                if (rec.Cantidad <= 0)
                    continue;

                var psParent = GetOrCreate(prEsRemovido ? padrePRRemovido : prRec);

                psParent.estructura.Add(new List<Campo>
        {
            new Campo { campo = "codigo",   valor = codigoHijo },
            new Campo { campo = "cantidad", valor = rec.Cantidad.ToString(CultureInfo.InvariantCulture) }
        });
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

        public static class Sg1Exclusions
        {
            private static readonly Dictionary<string, string> _processByExcludedPr =
                new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            public static void SetExcluded(string procesoP, string prCodigo)
            {
                procesoP = (procesoP ?? "").Trim();
                prCodigo = (prCodigo ?? "").Trim();

                if (string.IsNullOrWhiteSpace(procesoP) || string.IsNullOrWhiteSpace(prCodigo))
                    return;

                _processByExcludedPr[prCodigo] = procesoP; // PR -> P
            }

            public static bool TryGetProcessForExcludedPr(string prCodigo, out string procesoP)
            {
                prCodigo = (prCodigo ?? "").Trim();
                return _processByExcludedPr.TryGetValue(prCodigo, out procesoP);
            }

            public static void Clear() => _processByExcludedPr.Clear();

            // DEBUG útil
            public static IReadOnlyDictionary<string, string> Snapshot()
                => new Dictionary<string, string>(_processByExcludedPr, StringComparer.OrdinalIgnoreCase);
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


