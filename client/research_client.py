import requests
import sys
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()


# Paste your API Gateway URL here (no trailing slash)
BASE_URL = "https://YOUR-API-ID.execute-api.YOUR-REGION.amazonaws.com/YOUR-STAGE"

TREND_SORT_OPTIONS = ("emerging_score", "growth_rate", "paper_count")


def get_trends(sort_by=None):
    sort_by = sort_by or "emerging_score"
    if sort_by not in TREND_SORT_OPTIONS:
        sort_by = "emerging_score"
    url = f"{BASE_URL}/results/trends?sort={sort_by}"
    try:
        response = requests.get(url) 
        
        if response.status_code != 200:
            console.print(f"[red]API Error {response.status_code}: {response.text}[/red]")
            return

        data = response.json()
        
        if not isinstance(data, list):
            console.print(f"[red]Unexpected API Response: {data}[/red]")
            return

        table = Table(title=f"Top Research Trends (sorted by {sort_by})")
        table.add_column("Topic", style="cyan")
        table.add_column("Papers", style="magenta")
        table.add_column("Emerging Score", style="yellow")
        
        for item in data:
            table.add_row(
                str(item.get('topic', 'N/A')),
                str(item.get('paper_count', '0')),
                str(item.get('score_label', 'NEW'))
            )
        console.print(table)
        
    except Exception as e:
        console.print(f"[red]Client Error: {e}[/red]")

def get_clusters(cluster_id=None):
    url = f"{BASE_URL}/results/clusters"  
    if cluster_id:
        url += f"?cluster_id={cluster_id}"
        
    try:
        response = requests.get(url)
        
        if response.status_code != 200:
            console.print(f"[red] API Error {response.status_code}: {response.text}[/red]")
            return

        data = response.json()

        if isinstance(data, dict):
            items = data.get('clusters') or data.get('data') or [data]
        else:
            items = data

        if not isinstance(items, list) or len(items) == 0:
            console.print("[yellow] No cluster data found.[/yellow]")
            return

        if cluster_id:
            for c in items:
                console.print(Panel(
                    f"[bold green]Cluster ID: {c.get('cluster_id')}[/bold green] | [bold]Total Papers:[/bold] {c.get('size')}",
                    subtitle=f"Last Updated: {c.get('last_updated', 'N/A')}"
                ))
                
                console.print(f"[bold cyan]Top Topics:[/bold cyan] {c.get('top_topics')}")
                paper = c.get('representative_paper')
                if paper:
                    paper_content = (
                        f"[bold gold1]Title:[/bold gold1] {paper.get('title', 'N/A')}\n"
                        f"[bold blue]Paper ID:[/bold blue] {paper.get('paper_id', 'N/A')}\n"
                        f"[bold blue]Pub Date:[/bold blue] {paper.get('publication_date', 'No Date')}\n\n"
                        f"[italic white]Abstract Summary:[/italic white]\n"
                        f"{paper.get('abstract_summary') or 'No summary available in database.'}"
                    )
                    console.print(Panel(paper_content, title="Representative Paper Detail", border_style="green"))
                else:
                    console.print("[dim]No representative paper linked to this cluster.[/dim]")
        
        else:
            table = Table(title=" Research Clusters Overview", show_header=True, header_style="bold magenta")
            table.add_column("ID", style="dim", width=6)
            table.add_column("Size (Papers)", justify="right", style="green")
            table.add_column("Top Topics (Keywords)", style="cyan")

            for c in items:
                topics = str(c.get('top_topics', 'N/A'))
                len_topics = len(topics)
                short_topics = topics[1:len_topics-1]
                
                table.add_row(
                    str(c.get('cluster_id', 'N/A')),
                    str(c.get('size', '0')),
                    short_topics
                )
            
            console.print(table)
            console.print("\n[italic yellow] Hint: Run 'python research_client.py cluster 1' to see details & abstracts.[/italic yellow]")

    except Exception as e:
        console.print(f"[bold red]Client Error:[/bold red] {str(e)}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        console.print("[yellow]Usage: python research_client.py [trends [sort] | cluster [id]][/yellow]")
        console.print("[dim]  sort: emerging_score (default) | growth_rate | paper_count[/dim]")
        sys.exit(1)

    cmd = sys.argv[1].lower()
    if cmd == "trends":
        sort_arg = sys.argv[2] if len(sys.argv) > 2 else None
        get_trends(sort_arg)
    elif cmd == "cluster":
        cid = sys.argv[2] if len(sys.argv) > 2 else None
        get_clusters(cid)