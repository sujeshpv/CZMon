from django.shortcuts import render

# Create your views here.
def home(request):
    return render(request, "home1.html")

def dashboard(request):
    # sample context for the dashboard
    ctx = {
        "cluster_name": "Demo Cluster",
    }
    return render(request, "dashboard.html", ctx)