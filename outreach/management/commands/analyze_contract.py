"""
analyze_contract — CLI to force analysis on one or many Contract rows.

    python manage.py analyze_contract --contract-id 42
    python manage.py analyze_contract --pending          # all DOWNLOADED / DISCOVERED
    python manage.py analyze_contract --contract-id 42 --force  # re-analyze
"""
from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from outreach.models import Contract, Task
from outreach.tasks import analyze_contract as handler


class Command(BaseCommand):
    help = "Run the multi-agent analyzer over pending Contract rows."

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--contract-id",
            type=int,
            help="Analyze just this contract id.",
        )
        parser.add_argument(
            "--pending",
            action="store_true",
            help="Analyze every Contract in status=DISCOVERED or DOWNLOADED.",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Re-analyze even if status=COMPLETED.",
        )

    def handle(self, *args, **opts) -> None:
        if opts["contract_id"]:
            contracts = list(Contract.objects.filter(pk=opts["contract_id"]))
            if not contracts:
                raise CommandError(f"Contract id {opts['contract_id']} not found")
        elif opts["pending"]:
            contracts = list(
                Contract.objects.filter(
                    status__in=[
                        Contract.Status.DISCOVERED,
                        Contract.Status.DOWNLOADED,
                    ]
                )
            )
            if not contracts:
                self.stdout.write("No pending contracts.")
                return
        else:
            raise CommandError("Pass --contract-id <id> or --pending")

        if opts["force"]:
            for c in contracts:
                if c.status == Contract.Status.COMPLETED:
                    c.status = Contract.Status.DOWNLOADED
                    c.save(update_fields=["status", "updated_at"])

        for contract in contracts:
            task = Task(
                type=Task.Type.ANALYZE_CONTRACT,
                campaign=contract.lead.campaign,
                lead=contract.lead,
                source=contract.source,
                payload={"contract_id": contract.pk},
                scheduled_at=timezone.now(),
            )
            task.save()
            task.mark_running()
            try:
                handler.handle(task)
                task.mark_done()
            except Exception as exc:  # noqa: BLE001
                task.mark_failed(str(exc))
                self.stderr.write(f"[contract={contract.pk}] failed: {exc}")
                continue
            contract.refresh_from_db()
            self.stdout.write(
                self.style.SUCCESS(
                    f"[contract={contract.pk}] {contract.status} "
                    f"recommendation={contract.recommendation or '-'} "
                    f"score={contract.raw_analysis.get('overall_score') if isinstance(contract.raw_analysis, dict) else '-'}"
                )
            )
