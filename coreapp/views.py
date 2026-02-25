from django.shortcuts import render
from django.shortcuts import redirect
from django.urls import reverse
from django.conf import settings as dj_settings
import json
import os
import re
import ipaddress
from urllib.parse import quote_plus

# Create your views here.
def home(request):
    return render(request, "home1.html")

def get_endpoints():
    """Load and normalize endpoints from static/configurations/endpoints.json."""
    endpoints_path = os.path.join(dj_settings.BASE_DIR, "static", "configurations", "endpoints.json")
    if not os.path.exists(endpoints_path):
        return {"pcs": [], "pes": []}
    try:
        with open(endpoints_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:
        return {"pcs": [], "pes": []}

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
                normalized_pcs.append({"name": entry or "", "ip": entry})
            elif isinstance(entry, dict):
                name = (entry.get("name") or "").strip()
                ip = entry.get("ip") or entry.get("virtual_ip") or ""
                normalized_pcs.append({"name": name, "ip": ip})
    pcs = normalized_pcs

    normalized_pes = []
    if isinstance(pes, list):
        for entry in pes:
            if isinstance(entry, str):
                normalized_pes.append({"name": "", "ip": entry})
            elif isinstance(entry, dict):
                name = entry.get("name", "")
                ip = entry.get("ip") or entry.get("virtual_ip") or ""
                normalized_pes.append({"name": name, "ip": ip})
    pes = normalized_pes

    return {"pcs": pcs, "pes": pes}


def dashboard(request):
    data = get_endpoints()
    ctx = {
        "cluster_name": "Demo Cluster",
        "total_clusters": len(data["pes"]),
        "total_pcs": len(data["pcs"]),
    }
    return render(request, "dashboard.html", ctx)


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