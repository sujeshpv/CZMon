from django.shortcuts import render
from django.shortcuts import redirect
from django.urls import reverse
from django.conf import settings as dj_settings
from django.http import JsonResponse
from urllib.parse import quote_plus
from library.const import *
import json
import os
import re
import ipaddress
import sqlite3
from datetime import datetime, timedelta, timezone

# Create your views here.
def home(request):
    return render(request, "home1.html")

def get_endpoints():
    """Load and normalize endpoints from static/configurations/endpoints.json."""
    endpoints_path = os.path.join(dj_settings.BASE_DIR, STATIC, CONFIGURATIONS, ENDPOINTS_JSON)
    if not os.path.exists(endpoints_path):
        return {"pcs": [], "pes": []}
    try:
        with open(endpoints_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:
        return {"pcs": [], "pes": []}

    # Support AZ-based schema:
    # {
    #   "AZ1": [{"type":"PC", ...}, {"type":"PE", ...}]
    # }
    if isinstance(data, dict) and "pcs" not in data and "pes" not in data:
        pcs_from_az = []
        pes_from_az = []
        for az_key, entries in data.items():
            if not isinstance(entries, list):
                continue
            az_pcs = []
            az_pes = []
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                entry_type = str(entry.get("type") or "").strip().upper()
                normalized = {
                    "name": entry.get("name", ""),
                    "ip": entry.get("ip") or entry.get("virtual_ip") or "",
                    "user": entry.get("user", ""),
                    "password": entry.get("password", ""),
                    "az": az_key,
                }
                if entry_type == "PC":
                    az_pcs.append(dict(normalized))
                elif entry_type == "PE":
                    az_pes.append(dict(normalized))

            for pc in az_pcs:
                pc["pes"] = [dict(pe) for pe in az_pes]
                pcs_from_az.append(pc)

            for pe in az_pes:
                if az_pcs:
                    pe["pc_name"] = az_pcs[0].get("name", "")
                    pe["pc_ip"] = az_pcs[0].get("ip", "")
                pes_from_az.append(pe)

        data = {"pcs": pcs_from_az, "pes": pes_from_az}

    # Support AZ-based schema:
    # {
    #   "AZ1": [{"type":"PC", ...}, {"type":"PE", ...}]
    # }
    if isinstance(data, dict) and "pcs" not in data and "pes" not in data:
        pcs_from_az = []
        pes_from_az = []
        for az_key, entries in data.items():
            if not isinstance(entries, list):
                continue
            az_pcs = []
            az_pes = []
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                entry_type = str(entry.get("type") or "").strip().upper()
                normalized = {
                    "name": entry.get("name", ""),
                    "ip": entry.get("ip") or entry.get("virtual_ip") or "",
                    "user": entry.get("user", ""),
                    "password": entry.get("password", ""),
                    "az": az_key,
                }
                if entry_type == "PC":
                    az_pcs.append(dict(normalized))
                elif entry_type == "PE":
                    az_pes.append(dict(normalized))

            for pc in az_pcs:
                pc["pes"] = [dict(pe) for pe in az_pes]
                pcs_from_az.append(pc)

            for pe in az_pes:
                if az_pcs:
                    pe["pc_name"] = az_pcs[0].get("name", "")
                    pe["pc_ip"] = az_pcs[0].get("ip", "")
                pes_from_az.append(pe)

        data = {"pcs": pcs_from_az, "pes": pes_from_az}

    pcs = data.get("pcs", [])
    pes = data.get("pes", [])

    if isinstance(pcs, dict) and "virtual_ip" in pcs:
        v = pcs.get("virtual_ip")
        pcs = [v] if v else []
    if isinstance(pes, dict) and "virtual_ip" in pes:
        v = pes.get("virtual_ip")
        pes = [v] if v else []

    normalized_pcs = []
    if isinstance(pcs, list):
        for entry in pcs:
            if isinstance(entry, str):
                normalized_pcs.append({"name": entry or "", "ip": entry, "az": "", "pes": []})
            elif isinstance(entry, dict):
                name = (entry.get("name") or "").strip()
                ip = entry.get("ip") or entry.get("virtual_ip") or ""
                az = (entry.get("az") or "").strip()
                pc_pes = entry.get("pes", [])
                normalized_pc_pes = []
                if isinstance(pc_pes, list):
                    for pe in pc_pes:
                        if isinstance(pe, str) and pe.strip():
                            normalized_pc_pes.append(pe.strip())
                        elif isinstance(pe, dict):
                            pe_name = (pe.get("name") or "").strip()
                            pe_ip = (pe.get("ip") or pe.get("virtual_ip") or "").strip()
                            if pe_name and pe_ip:
                                if pe_name == pe_ip:
                                    normalized_pc_pes.append(pe_ip)
                                else:
                                    normalized_pc_pes.append(f"{pe_name} ({pe_ip})")
                            elif pe_name:
                                normalized_pc_pes.append(pe_name)
                            elif pe_ip:
                                normalized_pc_pes.append(pe_ip)
                normalized_pcs.append(
                    {"name": name, "ip": ip, "az": az, "pes": normalized_pc_pes}
                )
    pcs = normalized_pcs

    normalized_pes = []
    if isinstance(pes, list):
        for entry in pes:
            if isinstance(entry, str):
                normalized_pes.append({"name": "", "ip": entry, "pc_name": "", "pc_ip": ""})
            elif isinstance(entry, dict):
                name = entry.get("name", "")
                ip = entry.get("ip") or entry.get("virtual_ip") or ""
                pc_name = (entry.get("pc_name") or entry.get("pc") or "").strip()
                pc_ip = (entry.get("pc_ip") or "").strip()
                normalized_pes.append(
                    {"name": name, "ip": ip, "pc_name": pc_name, "pc_ip": pc_ip}
                )
    pes = normalized_pes

    return {"pcs": pcs, "pes": pes}

def _build_overview_context():
    """Build the template context shared by the Dashboard and Cluster Metrics pages.

    Produces the AZ / entity / PC-PE dropdown data sourced from endpoints.json
    together with the aggregate counters used by the Dashboard KPI cards. The
    same context is rendered on both pages; each template picks the pieces it
    needs.
    """
    def split_csv_values(raw_value):
        if not raw_value:
            return []
        if isinstance(raw_value, list):
            return [str(v).strip() for v in raw_value if str(v).strip()]
        return [part.strip() for part in str(raw_value).split(",") if part.strip()]

    def parse_az_candidates(values):
        az_pattern = re.compile(r"(AZ\d+)", re.IGNORECASE)
        az_values = set()
        for value in values:
            if not value:
                continue
            matches = az_pattern.findall(value)
            for match in matches:
                az_values.add(match.upper())
        if not az_values:
            return ["AZ1", "AZ2"]
        return sorted(az_values)

    def build_dropdown_data(endpoint_data):
        az_to_pcs = {}
        pc_to_pes = {}
        pc_to_clusters = {}
        all_clusters = set()
        all_pes = []
        all_pes_seen = set()
        az_pattern = re.compile(r"(AZ\d+)", re.IGNORECASE)

        def pe_display_value(name, ip):
            name = (name or "").strip()
            ip = (ip or "").strip()
            if name and ip and name == ip:
                return ip
            if name and ip and name != ip:
                return f"{name} ({ip})"
            return name or ip

        def pe_canonical(value):
            value = (value or "").strip()
            if not value:
                return ""
            match = re.match(r"^(.*)\(([^()]+)\)\s*$", value)
            if match:
                left = match.group(1).strip()
                right = match.group(2).strip()
                if left == right or not left:
                    return right.lower()
            return value.lower()

        def add_unique_pe(pe_value):
            canonical = pe_canonical(pe_value)
            if not canonical or canonical in all_pes_seen:
                return
            all_pes_seen.add(canonical)
            all_pes.append(pe_value)

        def pc_key(pc):
            return pc.get("name") or pc.get("ip")

        for pc in endpoint_data.get("pcs", []):
            key = pc_key(pc)
            if not key:
                continue
            name = pc.get("name") or pc.get("ip") or ""
            ip = pc.get("ip") or ""
            label = f"{name} ({ip})" if name and ip and name != ip else name or ip

            match = az_pattern.search(name)
            az = label

            az_to_pcs.setdefault(az, [])
            az_to_pcs[az].append({"key": key, "label": label})

            if pc.get("pes"):
                pc_to_pes.setdefault(key, [])
                for pe in pc.get("pes", []):
                    if pe and pe not in pc_to_pes[key]:
                        pc_to_pes[key].append(pe)

        for pe in endpoint_data.get("pes", []):
            pe_name = (pe.get("name") or "").strip()
            pe_ip = (pe.get("ip") or "").strip()
            if not pe_name and not pe_ip:
                continue
            pe_display = pe_display_value(pe_name, pe_ip)
            add_unique_pe(pe_display)

            target_pc = (pe.get("pc_name") or pe.get("pc_ip") or "").strip()
            if target_pc:
                pc_to_pes.setdefault(target_pc, [])
                if pe_display not in pc_to_pes[target_pc]:
                    pc_to_pes[target_pc].append(pe_display)

        # Fallback: if PEs are configured globally (no pc_name/pc_ip mapping),
        # show all configured PEs for selected PC.
        for pc in endpoint_data.get("pcs", []):
            key = pc_key(pc)
            ip = (pc.get("ip") or "").strip()
            if not key:
                continue
            if key not in pc_to_pes and all_pes:
                pc_to_pes[key] = list(all_pes)
            if ip and ip not in pc_to_pes and all_pes:
                pc_to_pes[ip] = list(all_pes)

        for az in az_to_pcs:
            az_to_pcs[az] = sorted(az_to_pcs[az], key=lambda item: item["label"].lower())
        for key in pc_to_pes:
            pc_to_pes[key] = sorted(pc_to_pes[key], key=lambda item: item.lower())

        return {
            "az_to_pcs": dict(sorted(az_to_pcs.items())),
            "pc_to_pes": pc_to_pes,
            "pc_to_clusters": {},
            "all_clusters": [],
            "all_pes": sorted(all_pes),
        }

    data = get_endpoints()
    dropdown_data = build_dropdown_data(data)
    az_options = []
    az_seen = set()
    az_to_pcs_indexed = {}
    az_to_entities = {}
    entity_kind_map = {}
    for idx, pc in enumerate(data.get("pcs", []), start=1):
        pc_name = str(pc.get("name") or "").strip()
        pc_ip = str(pc.get("ip") or "").strip()
        pc_key = pc_name or pc_ip
        if not pc_key:
            continue
        pc_label = f"{pc_name} ({pc_ip})" if pc_name and pc_ip and pc_name != pc_ip else (pc_name or pc_ip)
        az_key = str(pc.get("az") or "").strip() or f"AZ{idx}"
        if az_key not in az_seen:
            az_seen.add(az_key)
            az_options.append(az_key)
        az_to_pcs_indexed.setdefault(az_key, []).append({"key": pc_key, "label": pc_label})
        entities_for_az = az_to_entities.setdefault(az_key, [])
        if pc_ip:
            if pc_ip not in entities_for_az:
                entities_for_az.append(pc_ip)
            entity_kind_map[pc_ip] = "PC"
        for pe in pc.get("pes", []) or []:
            pe_ip = str((pe.get("ip") if isinstance(pe, dict) else pe) or "").strip()
            if pe_ip and pe_ip not in entities_for_az:
                entities_for_az.append(pe_ip)
            if pe_ip:
                entity_kind_map[pe_ip] = "PE"
    for pe in data.get("pes", []) or []:
        pe_ip = str((pe.get("ip") if isinstance(pe, dict) else pe) or "").strip()
        if pe_ip:
            entity_kind_map[pe_ip] = "PE"
    endpoint_entities = []
    endpoint_seen = set()
    for az in az_options:
        for entity in az_to_entities.get(az, []):
            text = str(entity or "").strip()
            if not text or text in endpoint_seen:
                continue
            endpoint_seen.add(text)
            endpoint_entities.append(text)
    ctx = {
        "cluster_name": "Demo Cluster",
        "total_clusters": len(data["pes"]),
        "total_pcs": len(data["pcs"]),
        "azs": az_options,
        "entity_types": ["Cluster", "SVM", "VM", "Container"],
        "entities": endpoint_entities,
        "az_to_pcs": az_to_pcs_indexed,
        "az_to_entities": az_to_entities,
        "pc_to_pes": dropdown_data["pc_to_pes"],
        "pc_to_clusters": dropdown_data["pc_to_clusters"],
        "all_clusters": dropdown_data["all_clusters"],
        "all_pes": dropdown_data["all_pes"],
        "entity_kind_map": entity_kind_map,
    }
    return ctx

def dashboard(request):
    return render(request, "dashboard.html", _build_overview_context())

def cluster_metrics_view(request):
    return render(request, "cluster_metrics.html", _build_overview_context())

def _resolve_time_range(range_key: str):
    now = datetime.now(timezone.utc)
    mapping = {
        "1h": timedelta(hours=1),
        "2h": timedelta(hours=2),
        "6h": timedelta(hours=6),
        "1d": timedelta(days=1),
        "2d": timedelta(days=2),
        "24h": timedelta(hours=24),
        "7d": timedelta(days=7),
        "14d": timedelta(days=14),
        "2w": timedelta(days=14),
    }
    delta = mapping.get((range_key or "").strip().lower())
    if not delta:
        return None, None
    start = (now - delta).isoformat()
    end = now.isoformat()
    return start, end

def _split_host_blocks(raw_output):
    blocks = []
    current_host = "unknown"
    current_lines = []
    marker_re = re.compile(r"^=+\s*([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)\s*=+$")
    for raw_line in (raw_output or "").splitlines():
        line = raw_line.rstrip("\n")
        marker = marker_re.match(line.strip())
        if marker:
            if current_lines:
                blocks.append({"host": current_host, "lines": current_lines})
                current_lines = []
            current_host = marker.group(1)
            continue
        if line.strip():
            current_lines.append(line)
    if current_lines:
        blocks.append({"host": current_host, "lines": current_lines})
    return blocks

def _parse_df_rows_from_blocks(blocks):
    parsed_rows = []
    if not isinstance(blocks, list):
        return parsed_rows
    for block in blocks:
        if not isinstance(block, dict):
            continue
        host = str(block.get("host") or "")
        lines = block.get("lines") or []
        header_found = False
        for raw_line in lines:
            line = str(raw_line).strip()
            if not line:
                continue
            if not header_found:
                if line.startswith("Filesystem") and "Use%" in line:
                    header_found = True
                continue
            parts = line.split()
            if len(parts) < 6:
                continue
            use_col = parts[4]
            if not use_col.endswith("%"):
                continue
            try:
                use_pct = float(use_col.rstrip("%"))
            except Exception:
                continue
            parsed_rows.append(
                {
                    "host": host,
                    "use_pct": use_pct,
                    "mount": " ".join(parts[5:]),
                }
            )
    return parsed_rows

def pe_partition_series_api(request):
    """
    Return PE partition usage time-series within selected time range.
    """
    cluster = (request.GET.get("cluster") or "").strip()
    entity = (request.GET.get("entity") or "").strip()
    node = (request.GET.get("node") or "").strip()
    partition = (request.GET.get("partition") or "").strip()
    start = (request.GET.get("start") or "").strip()
    end = (request.GET.get("end") or "").strip()
    range_key = (request.GET.get("range") or "").strip()
    if range_key and not start and not end:
        start, end = _resolve_time_range(range_key)

    db_path = os.path.join(dj_settings.BASE_DIR, "metrics.db")
    if not os.path.exists(db_path):
        return JsonResponse({"series": [], "range": range_key or "custom", "unit": "%"})

    series_map = {}
    pe_fallback_used = False
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            tables = {
                row[0]
                for row in cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            if "cluster_version" not in tables:
                return JsonResponse({"series": [], "range": range_key or "custom", "unit": "%"})

            where = ["command = ?"]
            params = ["df -h"]
            if cluster:
                where.append("cluster_name = ?")
                params.append(cluster)
            if start:
                where.append("created_at >= ?")
                params.append(start)
            if end:
                where.append("created_at <= ?")
                params.append(end)

            query = f"""
                SELECT created_at, ip, output_json, output
                FROM cluster_version
                WHERE {' AND '.join(where)}
                ORDER BY created_at ASC
                LIMIT 2000
            """
            db_rows = cursor.execute(query, params).fetchall()
            parsed_points = []
            for created_at, ip, raw_json, raw_output in db_rows:
                parsed_rows = []
                try:
                    payload = json.loads(raw_json or "{}")
                except Exception:
                    payload = {}

                if isinstance(payload, dict):
                    if payload.get("parser") == "raw_host_blocks":
                        parsed_rows = _parse_df_rows_from_blocks(payload.get("blocks", []))
                    elif isinstance(payload.get("rows"), list):
                        for row in payload.get("rows"):
                            if not isinstance(row, dict):
                                continue
                            mount = str(row.get("mount") or row.get("filesystem") or "")
                            use_pct = row.get("use_pct")
                            if isinstance(use_pct, (int, float)):
                                parsed_rows.append(
                                    {"host": str(row.get("host") or ip or ""), "use_pct": float(use_pct), "mount": mount}
                                )

                if not parsed_rows and raw_output:
                    parsed_rows = _parse_df_rows_from_blocks(_split_host_blocks(raw_output))

                for row in parsed_rows:
                    mount = str(row.get("mount") or "").strip()
                    host = str(row.get("host") or ip or "").strip()
                    value = row.get("use_pct")
                    if not host or not mount or not isinstance(value, (int, float)):
                        continue
                    if partition and mount != partition:
                        continue
                    parsed_points.append(
                        {"ts": created_at, "value": float(value), "pe": host, "partition": mount}
                    )

            selected_target = node or entity
            if selected_target:
                filtered_points = [p for p in parsed_points if p["pe"] == selected_target]
                if filtered_points:
                    parsed_points = filtered_points
                elif entity and not node:
                    pe_fallback_used = bool(parsed_points)

            for point in parsed_points:
                key = f"{point['pe']}|{point['partition']}"
                series_map.setdefault(key, []).append(point)
    except Exception as err:
        return JsonResponse({"error": f"Failed loading PE partition series: {err}"}, status=500)

    series = []
    for series_key, points in sorted(series_map.items()):
        points.sort(key=lambda p: p.get("ts") or "")
        sample = points[0] if points else {}
        pe = str(sample.get("pe") or "")
        partition = str(sample.get("partition") or "")
        display_name = f"{pe} {partition}".strip()
        series.append({"name": display_name, "pe": pe, "partition": partition, "points": points})

    return JsonResponse(
        {
            "series": series,
            "pe": entity,
            "node": node,
            "partition": partition,
            "range": range_key or "custom",
            "unit": "%",
            "y_min": 0,
            "y_max": 100,
            "pe_fallback_used": pe_fallback_used,
        }
    )

def partition_nodes_api(request):
    """
    Return distinct node IPs seen in df -h blocks for selected time range.
    """
    cluster = (request.GET.get("cluster") or "").strip()
    start = (request.GET.get("start") or "").strip()
    end = (request.GET.get("end") or "").strip()
    range_key = (request.GET.get("range") or "").strip()
    if range_key and not start and not end:
        start, end = _resolve_time_range(range_key)

    db_path = os.path.join(dj_settings.BASE_DIR, "metrics.db")
    if not os.path.exists(db_path):
        return JsonResponse({"nodes": []})

    hosts = set()
    range_fallback_used = False
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            tables = {
                row[0]
                for row in cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            if "cluster_version" not in tables:
                return JsonResponse({"nodes": []})

            def collect_hosts(window_start="", window_end=""):
                local_hosts = set()
                where = ["command = ?"]
                params = ["df -h"]
                if cluster:
                    where.append("cluster_name = ?")
                    params.append(cluster)
                if window_start:
                    where.append("created_at >= ?")
                    params.append(window_start)
                if window_end:
                    where.append("created_at <= ?")
                    params.append(window_end)
                query = f"""
                    SELECT output_json, output, ip
                    FROM cluster_version
                    WHERE {' AND '.join(where)}
                    ORDER BY created_at DESC
                    LIMIT 2000
                """
                for raw_json, raw_output, row_ip in cursor.execute(query, params).fetchall():
                    parsed_rows = []
                    try:
                        payload = json.loads(raw_json or "{}")
                    except Exception:
                        payload = {}

                    if isinstance(payload, dict):
                        if payload.get("parser") == "raw_host_blocks":
                            parsed_rows = _parse_df_rows_from_blocks(payload.get("blocks", []))
                        elif isinstance(payload.get("rows"), list):
                            for row in payload.get("rows"):
                                if not isinstance(row, dict):
                                    continue
                                mount = str(row.get("mount") or row.get("filesystem") or "")
                                use_pct = row.get("use_pct")
                                if isinstance(use_pct, (int, float)):
                                    parsed_rows.append(
                                        {"host": str(row.get("host") or row_ip or ""), "use_pct": float(use_pct), "mount": mount}
                                    )

                    if not parsed_rows and raw_output:
                        parsed_rows = _parse_df_rows_from_blocks(_split_host_blocks(raw_output))

                    for row in parsed_rows:
                        host = str(row.get("host") or row_ip or "").strip()
                        if host:
                            local_hosts.add(host)
                return local_hosts

            hosts = collect_hosts(start, end)
            if not hosts and (start or end):
                hosts = collect_hosts("", "")
                range_fallback_used = bool(hosts)
    except Exception as err:
        return JsonResponse({"error": f"Failed loading partition nodes: {err}"}, status=500)

    return JsonResponse({"nodes": sorted(hosts), "range_fallback_used": range_fallback_used})

def cluster_metrics_options_api(request):
    """
    Return available cluster names for cluster-centric metrics view.
    """
    db_path = os.path.join(dj_settings.BASE_DIR, "metrics.db")
    if not os.path.exists(db_path):
        return JsonResponse({"clusters": []})

    clusters = set()
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            tables = {
                row[0]
                for row in cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }

            if "cli_timeseries" in tables:
                clusters.update(
                    row[0]
                    for row in cursor.execute(
                        "SELECT DISTINCT source_cluster FROM cli_timeseries WHERE source_cluster IS NOT NULL AND source_cluster != ''"
                    ).fetchall()
                    if row and row[0]
                )
            if "cluster_version" in tables:
                clusters.update(
                    row[0]
                    for row in cursor.execute(
                        "SELECT DISTINCT cluster_name FROM cluster_version WHERE cluster_name IS NOT NULL AND cluster_name != ''"
                    ).fetchall()
                    if row and row[0]
                )
            if "clusters" in tables:
                clusters.update(
                    row[0]
                    for row in cursor.execute(
                        "SELECT DISTINCT name FROM clusters WHERE name IS NOT NULL AND name != ''"
                    ).fetchall()
                    if row and row[0]
                )
    except Exception as err:
        return JsonResponse({"error": f"Failed loading clusters: {err}"}, status=500)

    return JsonResponse({"clusters": sorted(clusters)})

def cluster_metrics_summary_api(request):
    """
    Return partition summary for selected entity + time range.
    """
    cluster = (request.GET.get("cluster") or "").strip()
    entity = (request.GET.get("entity") or "").strip()

    start = (request.GET.get("start") or "").strip()
    end = (request.GET.get("end") or "").strip()
    range_key = (request.GET.get("range") or "").strip()
    if range_key and not start and not end:
        start, end = _resolve_time_range(range_key)

    db_path = os.path.join(dj_settings.BASE_DIR, "metrics.db")
    if not os.path.exists(db_path):
        return JsonResponse(
            {
                "cluster": cluster,
                "summary": {
                    "partition_space": None,
                },
                "latest_metrics": [],
            }
        )

    latest_by_metric = {}
    metric_samples = {}
    metric_units = {}
    has_window = bool(start or end)
    partition_metric_names = {"use_pct", "disk_usage_pct", "filesystem_use_pct", "partition_space_pct"}
    partition_candidates = []
    data_source = "none"
    entity_fallback_used = False

    def _parse_df_rows_from_blocks(blocks):
        parsed_rows = []
        if not isinstance(blocks, list):
            return parsed_rows
        for block in blocks:
            if not isinstance(block, dict):
                continue
            host = str(block.get("host") or "")
            lines = block.get("lines") or []
            header_found = False
            for raw_line in lines:
                line = str(raw_line).strip()
                if not line:
                    continue
                if not header_found:
                    if line.startswith("Filesystem") and "Use%" in line:
                        header_found = True
                    continue
                parts = line.split()
                if len(parts) < 6:
                    continue
                use_col = parts[4]
                if not use_col.endswith("%"):
                    continue
                try:
                    use_pct = float(use_col.rstrip("%"))
                except Exception:
                    continue
                parsed_rows.append(
                    {
                        "host": host,
                        "use_pct": use_pct,
                        "mount": " ".join(parts[5:]),
                    }
                )
        return parsed_rows

    def _split_host_blocks_from_output(raw_output):
        blocks = []
        current_host = "unknown"
        current_lines = []
        marker_re = re.compile(r"^=+\s*([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)\s*=+$")
        for raw_line in (raw_output or "").splitlines():
            line = raw_line.rstrip("\n")
            marker = marker_re.match(line.strip())
            if marker:
                if current_lines:
                    blocks.append({"host": current_host, "lines": current_lines})
                    current_lines = []
                current_host = marker.group(1)
                continue
            if line.strip():
                current_lines.append(line)
        if current_lines:
            blocks.append({"host": current_host, "lines": current_lines})
        return blocks

    def _parse_recovery_points_usage(raw_output):
        values = []
        text = str(raw_output or "")
        if not text:
            return values
        patterns = [
            r"recovery\s*points?\s*usage[^0-9]*([0-9]+(?:\.[0-9]+)?)\s*%",
            r"recovery[_\s-]*points?[_\s-]*usage(?:[_\s-]*(?:pct|percent|percentage))?[^0-9]*([0-9]+(?:\.[0-9]+)?)",
        ]
        for pattern in patterns:
            for match in re.findall(pattern, text, flags=re.IGNORECASE):
                try:
                    values.append(float(match))
                except Exception:
                    continue
        # Support standalone scalar lines like "27.33TB" or "65%".
        for match in re.findall(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*(%|[kKmMgGtTpP][bB])\s*$", text, flags=re.IGNORECASE | re.MULTILINE):
            try:
                values.append(float(match[0]))
            except Exception:
                continue
        return values

    def _parse_recovery_rows_from_blocks(blocks):
        rows = []
        if not isinstance(blocks, list):
            return rows
        for block in blocks:
            if not isinstance(block, dict):
                continue
            host = str(block.get("host") or "")
            lines = block.get("lines") or []
            for raw_line in lines:
                line = str(raw_line or "").strip()
                if not line:
                    continue
                # Accept only explicit unit values (e.g. 27.33TB, 65.1%).
                match = re.search(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*(%|[kKmMgGtTpP][bB])\s*$", line)
                if not match:
                    continue
                try:
                    value = float(match.group(1))
                except Exception:
                    continue
                unit = (match.group(2) or "").upper()
                metric_name = "recovery_points_usage_pct" if unit == "%" else "recovery_points_usage"
                rows.append(
                    {
                        "host": host,
                        metric_name: value,
                        "dimension": unit,
                    }
                )
                break
        return rows

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            tables = {
                row[0]
                for row in cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }

            rows = []
            if "cli_timeseries" in tables:
                where = []
                params = []
                if cluster:
                    where.append("source_cluster = ?")
                    params.append(cluster)
                if entity:
                    where.append("(host_ip = ? OR source_ip = ? OR dimension_value = ?)")
                    params.extend([entity, entity, entity])
                if start:
                    where.append("created_at >= ?")
                    params.append(start)
                if end:
                    where.append("created_at <= ?")
                    params.append(end)
                query = f"""
                    SELECT created_at, metric_name, metric_value, host_ip, dimension_value
                    FROM cli_timeseries
                    {"WHERE " + " AND ".join(where) if where else ""}
                    ORDER BY created_at ASC
                """
                rows = cursor.execute(query, params).fetchall()
                if rows:
                    data_source = "cli_timeseries"

            # fallback from cluster_version.output_json if timeseries empty
            if not rows and "cluster_version" in tables:
                cols = {
                    row[1]
                    for row in cursor.execute("PRAGMA table_info(cluster_version)").fetchall()
                }
                if "output_json" in cols:
                    where = []
                    params = []
                    if cluster:
                        where.append("cluster_name = ?")
                        params.append(cluster)
                    if start:
                        where.append("created_at >= ?")
                        params.append(start)
                    if end:
                        where.append("created_at <= ?")
                        params.append(end)
                    query = f"""
                        SELECT created_at, ip, command, output_json
                        FROM cluster_version
                        {"WHERE " + " AND ".join(where) if where else ""}
                        ORDER BY created_at ASC
                    """
                    for created_at, ip, command_name, raw_json in cursor.execute(query, params).fetchall():
                        try:
                            payload = json.loads(raw_json or "{}")
                        except Exception:
                            continue
                        parsed_rows = []
                        if isinstance(payload, dict):
                            if isinstance(payload.get("rows"), list) and payload.get("rows"):
                                parsed_rows = payload.get("rows")
                            elif payload.get("parser") == "raw_host_blocks":
                                blocks = payload.get("blocks", [])
                                parsed_rows = _parse_df_rows_from_blocks(blocks)
                                parsed_rows.extend(_parse_recovery_rows_from_blocks(blocks))

                        for parsed_row in parsed_rows:
                            if not isinstance(parsed_row, dict):
                                continue
                            host_ip = str(parsed_row.get("host") or ip or "")
                            if entity and host_ip != entity:
                                continue
                            dimension_value = (
                                str(parsed_row.get("mount") or parsed_row.get("filesystem") or parsed_row.get("dimension") or "")
                            )
                            for metric_name, metric_value in parsed_row.items():
                                if metric_name == "host":
                                    continue
                                if isinstance(metric_value, (int, float)):
                                    rows.append(
                                        (
                                            created_at,
                                            metric_name,
                                            float(metric_value),
                                            host_ip,
                                            dimension_value,
                                        )
                                    )
                    if rows:
                        data_source = "cluster_version.output_json"

            # If no rows matched entity, fallback to cluster+time aggregate view.
            if not rows and entity:
                if "cli_timeseries" in tables:
                    where = []
                    params = []
                    if cluster:
                        where.append("source_cluster = ?")
                        params.append(cluster)
                    if start:
                        where.append("created_at >= ?")
                        params.append(start)
                    if end:
                        where.append("created_at <= ?")
                        params.append(end)
                    query = f"""
                        SELECT created_at, metric_name, metric_value, host_ip, dimension_value
                        FROM cli_timeseries
                        {"WHERE " + " AND ".join(where) if where else ""}
                        ORDER BY created_at ASC
                    """
                    rows = cursor.execute(query, params).fetchall()
                    if rows:
                        data_source = "cli_timeseries (entity fallback)"
                        entity_fallback_used = True

            for created_at, metric_name, metric_value, host_ip, dimension_value in rows:
                try:
                    numeric_value = float(metric_value)
                except Exception:
                    continue

                metric_samples.setdefault(metric_name, []).append(numeric_value)
                metric_units.setdefault(metric_name, []).append(str(dimension_value or "").strip())

                if (
                    metric_name in partition_metric_names
                    and str(dimension_value or "").strip() == "/"
                ):
                    partition_candidates.append(
                        {
                            "ts": created_at,
                            "value": numeric_value,
                            "host": host_ip,
                            "dimension": "/",
                        }
                    )

                existing = latest_by_metric.get(metric_name)
                if not existing:
                    latest_by_metric[metric_name] = {
                        "ts": created_at,
                        "value": numeric_value,
                        "host": host_ip,
                        "dimension": dimension_value,
                    }
                    continue

                # Prefer newer timestamps. For same timestamp snapshots (e.g. df -h),
                # keep the larger value for the same metric so 0% bootstrap rows
                # do not override meaningful partition utilization.
                if created_at > existing["ts"] or (
                    created_at == existing["ts"] and numeric_value > float(existing["value"])
                ):
                    latest_by_metric[metric_name] = {
                        "ts": created_at,
                        "value": numeric_value,
                        "host": host_ip,
                        "dimension": dimension_value,
                    }

            # Direct fallback: read persisted `df -h` raw output from DB and extract Use%.
            # This path guarantees partition metric from cluster_version even if generic parsing misses.
            if "cluster_version" in tables:
                where = ["command = ?"]
                params = ["df -h"]
                if cluster:
                    where.append("cluster_name = ?")
                    params.append(cluster)
                if start:
                    where.append("created_at >= ?")
                    params.append(start)
                if end:
                    where.append("created_at <= ?")
                    params.append(end)
                df_query = f"""
                    SELECT created_at, output
                    FROM cluster_version
                    WHERE {' AND '.join(where)}
                    ORDER BY created_at DESC
                    LIMIT 200
                """
                for created_at, raw_output in cursor.execute(df_query, params).fetchall():
                    df_rows = _parse_df_rows_from_blocks(_split_host_blocks_from_output(raw_output))
                    if not df_rows:
                        continue
                    entity_rows = [r for r in df_rows if entity and str(r.get("host") or "") == entity]
                    applicable_rows = entity_rows if entity_rows else df_rows
                    # Root partition only
                    root_rows = [
                        r for r in applicable_rows
                        if (r.get("mount") or "").strip() == "/" and isinstance(r.get("use_pct"), (int, float))
                    ]
                    use_values = [r.get("use_pct") for r in root_rows]
                    if not use_values:
                        continue
                    # one root row per host snapshot, keep max as safe tie-break
                    max_use = max(use_values)
                    if "use_pct" not in latest_by_metric or created_at > latest_by_metric["use_pct"]["ts"]:
                        latest_by_metric["use_pct"] = {
                            "ts": created_at,
                            "value": float(max_use),
                            "host": entity if entity_rows else "cluster",
                            "dimension": "/",
                        }
                    partition_candidates.append(
                        {
                            "ts": created_at,
                            "value": float(max_use),
                            "host": entity if entity_rows else "cluster",
                            "dimension": "/",
                        }
                    )
                    if data_source == "none":
                        data_source = "cluster_version.output(raw df -h)"
    except Exception as err:
        return JsonResponse({"error": f"Failed loading cluster metrics: {err}"}, status=500)

    aliases = {
        "partition_space": ["use_pct", "disk_usage_pct", "filesystem_use_pct", "partition_space_pct"],
        "cpu_usage": ["cpu_usage", "cpu_pct", "cpu_utilization_pct", "cpu_percent"],
    }

    summary = {
        "partition_space": None,
        "cpu_usage": None,
    }
    # Partition space should reflect root mount '/' only.
    root_partition_value = None
    root_partition_ts = ""
    dedup_partition_candidates = []
    seen_partition_points = set()
    for point in partition_candidates:
        key = (
            point.get("ts") or "",
            point.get("host") or "",
            point.get("dimension") or "",
            float(point.get("value") or 0.0),
        )
        if key in seen_partition_points:
            continue
        seen_partition_points.add(key)
        dedup_partition_candidates.append(point)
    partition_candidates = dedup_partition_candidates
    if partition_candidates and not has_window:
        chosen = max(partition_candidates, key=lambda x: x.get("ts") or "")
        root_partition_value = chosen.get("value")
        root_partition_ts = chosen.get("ts") or ""

    # Strong fallback: read persisted df -h output directly and pick root '/' usage.
    if root_partition_value is None:
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                where = ["command = ?"]
                params = ["df -h"]
                if cluster:
                    where.append("cluster_name = ?")
                    params.append(cluster)
                if start:
                    where.append("created_at >= ?")
                    params.append(start)
                if end:
                    where.append("created_at <= ?")
                    params.append(end)
                order_dir = "ASC" if has_window else "DESC"
                query = f"""
                    SELECT created_at, output
                    FROM cluster_version
                    WHERE {' AND '.join(where)}
                    ORDER BY created_at {order_dir}
                    LIMIT 200
                """
                fallback_values = []
                for created_at, raw_output in cursor.execute(query, params).fetchall():
                    df_rows = _parse_df_rows_from_blocks(_split_host_blocks_from_output(raw_output))
                    if entity:
                        df_rows = [r for r in df_rows if str(r.get("host") or "") == entity]
                    root_rows = [
                        r for r in df_rows
                        if (r.get("mount") or "").strip() == "/" and isinstance(r.get("use_pct"), (int, float))
                    ]
                    if not root_rows:
                        continue
                    row_value = max(r.get("use_pct") for r in root_rows)
                    fallback_values.append(float(row_value))
                    if not has_window:
                        root_partition_value = float(row_value)
                        root_partition_ts = created_at
                        break
                if has_window and fallback_values:
                    root_partition_value = sum(fallback_values) / len(fallback_values)
        except Exception:
            pass
    if has_window and partition_candidates:
        partition_values = [float(p.get("value") or 0.0) for p in partition_candidates]
        if partition_values:
            root_partition_value = sum(partition_values) / len(partition_values)
    summary["partition_space"] = root_partition_value

    for summary_key, candidates in aliases.items():
        if summary_key == "partition_space":
            continue
        if has_window:
            for metric_name in candidates:
                samples = metric_samples.get(metric_name) or []
                if samples:
                    summary[summary_key] = sum(samples) / len(samples)
                    break
        else:
            for metric_name in candidates:
                if metric_name in latest_by_metric:
                    summary[summary_key] = latest_by_metric[metric_name]["value"]
                    break

    latest_metrics = []
    for metric_name, meta in sorted(latest_by_metric.items()):
        latest_metrics.append(
            {
                "metric_name": metric_name,
                "value": meta["value"],
                "ts": meta["ts"],
                "host": meta["host"],
                "dimension": meta["dimension"],
            }
        )

    return JsonResponse(
        {
            "cluster": cluster,
            "entity": entity,
            "range": range_key or "custom",
            "data_source": data_source,
            "matched_rows": len(rows),
            "entity_fallback_used": entity_fallback_used,
            "summary": summary,
            "latest_metrics": latest_metrics,
        }
    )

def settings_view(request):
    endpoints_path = os.path.join(dj_settings.BASE_DIR, "static", "configurations", "endpoints.json")

    def save_endpoints(data):
        dirpath = os.path.dirname(endpoints_path)
        os.makedirs(dirpath, exist_ok=True)
        # write atomically: write to temp file then replace
        tmp_path = endpoints_path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, endpoints_path)

    def is_valid_host(value: str) -> bool:
        if not value:
            return False
        value = value.strip()
        # try IP address first
        try:
            ipaddress.ip_address(value)
            return True
        except Exception:
            pass

        # validate FQDN/hostname (simple)
        if len(value) > 255:
            return False
        # labels: alnum + hyphen, not start/end with hyphen
        fqdn_re = re.compile(r"^(?=.{1,255}$)([A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*\.?$")
        return bool(fqdn_re.match(value))

    def redirect_with_msg(name, message, level="info"):
        qs = f"?msg={quote_plus(message)}&level={quote_plus(level)}"
        return redirect(reverse(name) + qs)

    if request.method == "POST":
        data = get_endpoints()
        # Add PC
        if "add_pc" in request.POST:
            pc_name = request.POST.get("pc_name", "").strip()
            pc_ip = request.POST.get("pc_ip", "").strip()
            if not pc_name:
                return redirect_with_msg("settings", "PC Name is required and cannot be blank", "error")
            if not pc_ip:
                return redirect_with_msg("settings", "Please provide PC Virtual IP/FQDN", "error")
            if not is_valid_host(pc_ip):
                return redirect_with_msg("settings", f"Invalid PC IP/FQDN: {pc_ip}", "error")

            pcs_list = data.get("pcs", [])
            existing_ips = [p.get("ip") for p in pcs_list if isinstance(p, dict)]
            existing_names = [p.get("name", "").strip() for p in pcs_list if isinstance(p, dict)]
            if pc_ip in existing_ips:
                return redirect_with_msg("settings", f"PC already configured: {pc_ip}", "info")
            if pc_name in existing_names:
                return redirect_with_msg("settings", f"PC Name must be unique: {pc_name}", "error")

            data.setdefault("pcs", []).append({"name": pc_name, "ip": pc_ip})
            save_endpoints(data)
            return redirect_with_msg("settings", f"PC added: {pc_name} ({pc_ip})", "success")

        # Add PE
        if "add_pe" in request.POST:
            pe_ip = request.POST.get("pe_ip", "").strip()
            pe_name = request.POST.get("pe_name", "").strip()
            if not pe_ip:
                return redirect_with_msg("settings", "Please provide PE Virtual IP/FQDN", "error")
            if not is_valid_host(pe_ip):
                return redirect_with_msg("settings", f"Invalid PE IP/FQDN: {pe_ip}", "error")
            if not pe_name:
                return redirect_with_msg("settings", "PE Name is required", "error")

            # check duplicates by ip and name
            pes_list = data.get("pes", [])
            existing_ips = [ (p.get("ip") if isinstance(p, dict) else p) for p in pes_list ]
            existing_names = [ p.get("name", "") for p in pes_list if isinstance(p, dict) ]
            if pe_ip in existing_ips:
                return redirect_with_msg("settings", f"PE already configured: {pe_ip}", "info")
            if pe_name in existing_names:
                return redirect_with_msg("settings", f"PE Name already exists: {pe_name}", "info")

            # store as dict with name and ip
            data.setdefault("pes", []).append({"name": pe_name, "ip": pe_ip})
            save_endpoints(data)
            disp = f"{pe_name} ({pe_ip})"
            return redirect_with_msg("settings", f"PE added: {disp}", "success")

        # Delete PC
        if "delete_pc" in request.POST:
            ip = request.POST.get("ip", "").strip()
            if ip:
                pcs_list = data.get("pcs", [])
                new_pcs = [p for p in pcs_list if isinstance(p, dict) and p.get("ip") != ip]
                if len(new_pcs) != len(pcs_list):
                    data["pcs"] = new_pcs
                    save_endpoints(data)
                    return redirect_with_msg("settings", f"PC removed: {ip}", "success")
            return redirect_with_msg("settings", f"PC not found: {ip}", "error")

        # Delete PE
        if "delete_pe" in request.POST:
            ip = request.POST.get("ip", "").strip()
            if ip:
                pes_list = data.get("pes", [])
                new_pes = []
                removed = False
                for entry in pes_list:
                    if isinstance(entry, dict):
                        if entry.get("ip") == ip:
                            removed = True
                            continue
                        new_pes.append(entry)
                    else:
                        if entry == ip:
                            removed = True
                            continue
                        new_pes.append(entry)
                data["pes"] = new_pes
                if removed:
                    save_endpoints(data)
                    return redirect_with_msg("settings", f"PE removed: {ip}", "success")
            return redirect_with_msg("settings", f"PE not found: {ip}", "error")

    # load endpoints for display
    data = get_endpoints()
    # read optional message from querystring (used instead of Django messages)
    msg = request.GET.get("msg")
    level = request.GET.get("level", "info")
    ctx = {
        "pcs": data.get("pcs", []),
        "pes": data.get("pes", []),
        "msg": msg,
        "level": level,
    }
    return render(request, "settings.html", ctx)
