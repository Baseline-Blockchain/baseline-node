"""Layout helpers responsible for building the wallet GUI controls."""

from __future__ import annotations

import contextlib
import tkinter as tk
from tkinter import ttk

from .styles import ASSET_DIR, PALETTE, lighten


class LayoutMixin:
    """Mixin that drives the widget layout and presentation."""

    def _schedule_responsive_layout(self) -> None:
        if getattr(self, "_resize_after", None) is not None:
            with contextlib.suppress(Exception):
                self.after_cancel(self._resize_after)
        self._resize_after = self.after(100, self._apply_responsive_layout)

    def _apply_responsive_layout(self) -> None:
        self._resize_after = None
        self._resize_tree_columns()
        self._resize_overview_wrap()

    def _resize_overview_wrap(self) -> None:
        chain_card = getattr(self, "_overview_chain_card", None)
        if not chain_card or not chain_card.winfo_exists():
            return
        wrap_width = max(220, chain_card.winfo_width() - 40)
        for label in (
            getattr(self, "_overview_hash_label", None),
            getattr(self, "_overview_genesis_label", None),
        ):
            if label and label.winfo_exists():
                label.configure(wraplength=wrap_width)

    def _resize_tree_columns(self) -> None:
        def resize(tree: ttk.Treeview | None, spec: dict[str, int], stretch_column: str) -> None:
            if not tree or not tree.winfo_exists():
                return
            width = tree.winfo_width()
            if width <= 1:
                return
            fixed = sum(v for k, v in spec.items() if k != stretch_column)
            remaining = max(spec.get(stretch_column, 100), width - fixed - 20)
            for name, col_width in spec.items():
                if name == stretch_column:
                    tree.column(name, width=remaining, minwidth=100, stretch=True)
                else:
                    tree.column(name, width=col_width, minwidth=col_width, stretch=False)

        resize(
            self.address_tree,
            {"address": 360, "label": 160, "spendable": 100, "balance": 120},
            "address",
        )
        resize(
            self.tx_tree,
            {"time": 150, "txid": 260, "category": 120, "amount": 150, "confirmations": 90},
            "txid",
        )
        resize(
            self.schedule_tree,
            {
                "schedule": 190,
                "destination": 200,
                "amount": 110,
                "fee": 95,
                "lock_time": 120,
                "status": 100,
                "cancelable": 60,
            },
            "destination",
        )

    def _load_icon(self) -> None:
        """Load the packaged logo and apply it as the window icon."""

        png_path = ASSET_DIR / "logo.png"
        if png_path.exists():
            try:
                self._icon_image = tk.PhotoImage(file=str(png_path))
                self.iconphoto(False, self._icon_image)
                self.iconphoto(True, self._icon_image)
            except Exception as exc:  # pragma: no cover - best-effort asset
                print(f"[wallet-gui] Unable to load logo.png: {exc}")
        else:
            print("[wallet-gui] logo.png not found; using default Tk icon")

    def _apply_theme(self) -> None:
        """Configure ttk styles for the light palette."""

        style = ttk.Style()
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass

        self.option_add("*Foreground", PALETTE["text"])
        self.option_add("*Background", PALETTE["bg"])
        style.configure("TFrame", background=PALETTE["bg"])
        style.configure("Card.TFrame", background=PALETTE["panel"])
        style.configure("TLabel", background=PALETTE["bg"], foreground=PALETTE["text"])
        style.configure("Status.TLabel", background=PALETTE["status"], foreground=PALETTE["muted"])
        style.configure("StatusBar.TFrame", background=PALETTE["status"])
        style.configure("CardLabel.TLabel", background=PALETTE["panel"], foreground=PALETTE["muted"])
        style.configure(
            "MetricLabel.TLabel",
            background=PALETTE["panel"],
            foreground=PALETTE["text"],
            font=("Segoe UI", 12),
        )
        style.configure(
            "MetricLarge.TLabel",
            background=PALETTE["panel"],
            foreground=PALETTE["text"],
            font=("Segoe UI", 15),
        )
        style.configure(
            "MonoLabel.TLabel",
            background=PALETTE["panel"],
            foreground=PALETTE["text"],
            font=("Consolas", 10),
        )
        style.configure(
            "SectionHeading.TLabel",
            background=PALETTE["panel"],
            foreground=PALETTE["muted"],
            font=("Segoe UI", 9),
        )
        style.configure(
            "Primary.TButton",
            background=PALETTE["button"],
            foreground="#ffffff",
            borderwidth=0,
            focusthickness=1,
            focuscolor=PALETTE["highlight"],
            padding=(12, 6),
        )
        style.map(
            "Primary.TButton",
            background=[("active", lighten(PALETTE["button"]))],
            foreground=[("disabled", PALETTE["muted"])],
        )
        style.configure(
            "Secondary.TButton",
            background=PALETTE["tab_active"],
            foreground=PALETTE["text"],
            borderwidth=0,
            focusthickness=1,
            focuscolor=PALETTE["highlight"],
            padding=(12, 6),
        )
        style.map(
            "Secondary.TButton",
            background=[("active", lighten(PALETTE["tab_active"]))],
            foreground=[("disabled", PALETTE["muted"])],
        )
        style.configure(
            "ScheduledCancel.TButton",
            background="#1d4ed8",
            foreground="#ffffff",
            borderwidth=0,
            focusthickness=1,
            focuscolor=PALETTE["highlight"],
            padding=(12, 6),
        )
        style.map(
            "ScheduledCancel.TButton",
            background=[
                ("active", lighten("#1d4ed8")),
                ("disabled", lighten("#1d4ed8", 0.5)),
            ],
            foreground=[("disabled", "#e0f2fe")],
        )
        style.configure(
            "TEntry",
            fieldbackground=PALETTE["panel"],
            background=PALETTE["panel"],
            foreground=PALETTE["text"],
            bordercolor=PALETTE["accent"],
            insertcolor=PALETTE["text"],
            padding=4,
        )
        style.configure(
            "Form.TEntry",
            fieldbackground="#ffffff",
            background="#ffffff",
            foreground=PALETTE["text"],
            bordercolor=PALETTE["accent"],
            lightcolor=PALETTE["accent"],
            darkcolor=PALETTE["accent"],
            borderwidth=1,
            padding=4,
        )
        style.map(
            "Form.TEntry",
            fieldbackground=[("disabled", PALETTE["panel"]), ("readonly", "#ffffff"), ("focus", "#ffffff")],
        )
        style.configure(
            "Form.TCombobox",
            fieldbackground="#ffffff",
            background="#ffffff",
            foreground=PALETTE["text"],
            bordercolor=PALETTE["accent"],
            lightcolor=PALETTE["accent"],
            darkcolor=PALETTE["accent"],
            borderwidth=1,
            padding=4,
            arrowsize=16,
        )
        style.map(
            "Form.TCombobox",
            fieldbackground=[("readonly", "#ffffff")],
            foreground=[("disabled", PALETTE["muted"])],
        )
        style.configure(
            "TNotebook",
            background=PALETTE["bg"],
            borderwidth=0,
            tabmargins=(4, 4, 4, 0),
        )
        style.configure(
            "TNotebook.Tab",
            background=PALETTE["panel"],
            foreground=PALETTE["muted"],
            padding=(14, 6),
            borderwidth=0,
        )
        style.map(
            "TNotebook.Tab",
            background=[("selected", PALETTE["tab_active"])],
            foreground=[("selected", PALETTE["text"])],
        )
        style.configure(
            "Card.Treeview",
            background=PALETTE["panel"],
            fieldbackground=PALETTE["panel"],
            foreground=PALETTE["text"],
            rowheight=24,
            bordercolor=PALETTE["accent"],
        )
        style.configure(
            "Treeview.Heading",
            background=PALETTE["accent"],
            foreground=PALETTE["text"],
            padding=(6, 4),
        )
        style.map(
            "Card.Treeview",
            background=[("selected", PALETTE["highlight"])],
            foreground=[("selected", "#ffffff")],
        )
        style.map(
            "Treeview",
            background=[("selected", PALETTE["highlight"])],
            foreground=[("selected", "#ffffff")],
        )
        style.configure(
            "ScheduledCheck.TCheckbutton",
            background=PALETTE["panel"],
            foreground=PALETTE["text"],
        )
        style.map(
            "ScheduledCheck.TCheckbutton",
            background=[("active", PALETTE["panel"]), ("!active", PALETTE["panel"])],
            foreground=[("disabled", PALETTE["muted"])],
        )
        style.configure(
            "ScheduledScroll.TScrollbar",
            gripcount=0,
            background=PALETTE["accent"],
            troughcolor="#d9e3ff",
            bordercolor=PALETTE["accent"],
            arrowcolor=PALETTE["text"],
        )
        try:
            style.layout("ScheduledScroll.TScrollbar", style.layout("Vertical.TScrollbar"))
        except tk.TclError:
            pass

    def _build_layout(self) -> None:
        container = ttk.Frame(self, padding=(10, 10, 10, 10))
        container.pack(fill="both", expand=True)
        container.bind("<Configure>", lambda _event: self._schedule_responsive_layout())

        notebook = ttk.Notebook(container)
        notebook.pack(fill="both", expand=True)

        overview = ttk.Frame(notebook, padding=10)
        notebook.add(overview, text="Overview")
        self._build_overview_tab(overview)

        addresses = ttk.Frame(notebook, padding=10)
        notebook.add(addresses, text="Addresses")
        self._build_addresses_tab(addresses)

        history = ttk.Frame(notebook, padding=10)
        notebook.add(history, text="History")
        self._build_history_tab(history)

        scheduled = ttk.Frame(notebook, padding=10)
        notebook.add(scheduled, text="Scheduled Send")
        self._build_scheduled_tab(scheduled)

        send = ttk.Frame(notebook, padding=10)
        notebook.add(send, text="Send")
        self._build_send_tab(send)

        mempool = ttk.Frame(notebook, padding=10)
        notebook.add(mempool, text="Mempool")
        self._build_mempool_tab(mempool)

        status_bar = ttk.Frame(container, style="StatusBar.TFrame", padding=(10, 8))
        status_bar.pack(fill="x", pady=(8, 0))
        ttk.Label(status_bar, textvariable=self.status_var, style="Status.TLabel").pack(anchor="w")

    def _build_overview_tab(self, frame: ttk.Frame) -> None:
        stats = ttk.Frame(frame)
        stats.pack(fill="x")
        stat_cards = (
            ("BALANCE (BLINE)", self.balance_var),
            ("CHAIN HEIGHT", self.height_var),
            ("PEERS", self.peer_count_var),
            ("MEMPOOL", self.mempool_var),
        )
        for idx, (title, var) in enumerate(stat_cards):
            card = ttk.Frame(stats, style="Card.TFrame", padding=16)
            card.grid(row=0, column=idx, padx=(0 if idx == 0 else 10, 0), sticky="nsew")
            ttk.Label(card, text=title, style="SectionHeading.TLabel").pack(anchor="w")
            ttk.Label(card, textvariable=var, style="MetricLarge.TLabel").pack(anchor="w", pady=(4, 0))
            stats.columnconfigure(idx, weight=1)

        self.wallet_tip_label = ttk.Label(frame, textvariable=self.wallet_tip_var, style="CardLabel.TLabel", padding=(12, 4))
        self._update_wallet_tip(None)

        detail = ttk.Frame(frame, padding=(0, 12, 0, 0))
        detail.pack(fill="both", expand=True)
        detail.columnconfigure(0, weight=1)
        detail.columnconfigure(1, weight=1)
        detail.columnconfigure(2, weight=1)

        chain_card = ttk.Frame(detail, style="Card.TFrame", padding=16)
        self._overview_chain_card = chain_card
        chain_card.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        ttk.Label(chain_card, text="CHAIN STATUS", style="SectionHeading.TLabel").pack(anchor="w")
        self._add_overview_row(chain_card, "Sync Progress", self.chain_progress_var)
        self._add_overview_row(chain_card, "Difficulty", self.chain_difficulty_var)
        self._add_overview_row(chain_card, "Wallet Height", self.wallet_height_var)
        self._add_overview_row(chain_card, "Last Block Time", self.chain_time_var)
        self._overview_hash_label = self._add_overview_row(
            chain_card,
            "Best Block",
            self.chain_hash_var,
            style="MonoLabel.TLabel",
            wrap=280,
        )
        self._overview_genesis_label = self._add_overview_row(
            chain_card,
            "Genesis",
            self.chain_genesis_var,
            style="MonoLabel.TLabel",
            wrap=280,
        )

        network_card = ttk.Frame(detail, style="Card.TFrame", padding=16)
        network_card.grid(row=0, column=1, sticky="nsew", padx=8)
        ttk.Label(network_card, text="NETWORK", style="SectionHeading.TLabel").pack(anchor="w")
        self._add_overview_row(network_card, "Connections", self.peer_count_var)
        self._add_overview_row(network_card, "Network", self.network_name_var)
        self._add_overview_row(network_card, "Client Version", self.network_version_var)
        self._add_overview_row(network_card, "Relay Fee", self.network_fee_var)

        mempool_card = ttk.Frame(detail, style="Card.TFrame", padding=16)
        mempool_card.grid(row=0, column=2, sticky="nsew", padx=(8, 0))
        ttk.Label(mempool_card, text="MEMPOOL", style="SectionHeading.TLabel").pack(anchor="w")
        self._add_overview_row(mempool_card, "Transactions", self.mempool_var)
        self._add_overview_row(mempool_card, "Usage", self.mempool_usage_var)
        self._add_overview_row(mempool_card, "Min Fee", self.mempool_minfee_var)

    def _add_overview_row(
        self,
        container: ttk.Frame,
        label: str,
        var: tk.StringVar,
        *,
        style: str = "MetricLabel.TLabel",
        wrap: int | None = None,
    ) -> ttk.Label:
        wrapper = ttk.Frame(container, style="Card.TFrame")
        wrapper.pack(fill="x", pady=(8, 0))
        ttk.Label(wrapper, text=label, style="SectionHeading.TLabel").pack(anchor="w")
        lbl = ttk.Label(wrapper, textvariable=var, style=style)
        if wrap:
            lbl.configure(wraplength=wrap, justify="left")
        lbl.pack(anchor="w")
        return lbl

    def _update_wallet_tip(self, message: str | None) -> None:
        if not self.wallet_tip_label:
            return
        if message:
            self.wallet_tip_var.set(message)
            if not self.wallet_tip_label.winfo_ismapped():
                self.wallet_tip_label.pack(fill="x", pady=(12, 0))
        else:
            self.wallet_tip_var.set("")
            if self.wallet_tip_label.winfo_ismapped():
                self.wallet_tip_label.pack_forget()

    def _build_addresses_tab(self, frame: ttk.Frame) -> None:
        columns = ("address", "label", "spendable", "balance")
        tree = ttk.Treeview(frame, columns=columns, show="headings", height=18, style="Card.Treeview")
        self.address_tree = tree
        tree.heading("address", text="Address")
        tree.heading("label", text="Label")
        tree.heading("spendable", text="Spendable")
        tree.heading("balance", text="Balance")
        tree.column("address", width=360, minwidth=180, stretch=True)
        tree.column("label", width=160, minwidth=120, stretch=False)
        tree.column("spendable", width=100, minwidth=90, anchor="center", stretch=False)
        tree.column("balance", width=120, minwidth=90, anchor="e", stretch=False)
        odd_color = lighten(PALETTE["accent"], 0.35)
        tree.tag_configure("even", background=PALETTE["panel"], foreground=PALETTE["text"])
        tree.tag_configure("odd", background=odd_color, foreground=PALETTE["text"])
        tree.tag_configure("offline", background=PALETTE["panel"], foreground=PALETTE["muted"])
        tree.bind("<Double-1>", self._copy_selected_address)
        tree.bind("<Control-c>", self._copy_selected_address)
        tree.pack(fill="both", expand=True)
        notice = ttk.Label(frame, textvariable=self._address_notice_var, style="CardLabel.TLabel")
        notice.pack(anchor="w", pady=(6, 0))
        notice.pack_forget()
        self._address_notice_label = notice

    def _build_history_tab(self, frame: ttk.Frame) -> None:
        columns = ("time", "txid", "category", "amount", "confirmations")
        tree = ttk.Treeview(frame, columns=columns, show="headings", height=18, style="Card.Treeview")
        self.tx_tree = tree
        tree.heading("time", text="Time")
        tree.heading("txid", text="TxID")
        tree.heading("category", text="Type")
        tree.heading("amount", text="Amount (BLINE)")
        tree.heading("confirmations", text="Conf")
        tree.column("time", width=150, minwidth=120, stretch=False)
        tree.column("txid", width=260, minwidth=220, stretch=True)
        tree.column("category", width=120, minwidth=90, anchor="center", stretch=False)
        tree.column("amount", width=150, minwidth=100, anchor="e", stretch=False)
        tree.column("confirmations", width=90, minwidth=60, anchor="center", stretch=False)
        tree.tag_configure("offline", background=PALETTE["panel"], foreground=PALETTE["muted"])
        tree.bind("<Double-1>", self._show_transaction_details)
        tree.pack(fill="both", expand=True)

    def _build_scheduled_tab(self, frame: ttk.Frame) -> None:
        tree_frame = ttk.Frame(frame)
        tree_frame.pack(fill="both", expand=True)
        columns = ("schedule", "destination", "amount", "fee", "lock_time", "status", "cancelable")
        tree = ttk.Treeview(tree_frame, columns=columns, show="headings", height=12, style="Card.Treeview")
        self.schedule_tree = tree
        tree.heading("schedule", text="Schedule ID")
        tree.heading("destination", text="Destination")
        tree.heading("amount", text="Amount (BLINE)")
        tree.heading("fee", text="Fee (BLINE)")
        tree.heading("lock_time", text="Scheduled Time")
        tree.heading("status", text="Status")
        tree.heading("cancelable", text="Cancelable")
        tree.column("schedule", width=190, minwidth=160, stretch=False)
        tree.column("destination", width=200, minwidth=160, stretch=True)
        tree.column("amount", width=110, minwidth=90, anchor="e", stretch=False)
        tree.column("fee", width=95, minwidth=80, anchor="e", stretch=False)
        tree.column("lock_time", width=120, minwidth=90, anchor="center", stretch=False)
        tree.column("status", width=100, minwidth=80, anchor="center", stretch=False)
        tree.column("cancelable", width=60, minwidth=60, anchor="center", stretch=False)
        odd_color = lighten(PALETTE["accent"], 0.35)
        tree.tag_configure("even", background=PALETTE["panel"], foreground=PALETTE["text"])
        tree.tag_configure("odd", background=odd_color, foreground=PALETTE["text"])
        tree.tag_configure("offline", background=PALETTE["panel"], foreground=PALETTE["muted"])
        tree.bind("<<TreeviewSelect>>", self._on_schedule_selection)
        scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview, style="ScheduledScroll.TScrollbar")
        tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        tree.pack(fill="both", expand=True)

        cancel_frame = ttk.Frame(frame)
        cancel_frame.pack(fill="x", pady=(6, 12))
        self._schedule_cancel_button = ttk.Button(
            cancel_frame,
            text="Cancel Selected Schedule",
            style="ScheduledCancel.TButton",
            command=self._cancel_selected_schedule,
        )
        self._schedule_cancel_button.pack(side="left")
        self._schedule_cancel_button.state(["disabled"])

        card = ttk.Frame(frame, style="Card.TFrame", padding=16)
        card.pack(fill="x")
        card.columnconfigure(0, weight=1)
        card.columnconfigure(1, weight=1)
        ttk.Label(card, text="From Address", style="SectionHeading.TLabel").grid(row=0, column=0, columnspan=2, sticky="w")
        self.schedule_from_combo = ttk.Combobox(
            card,
            textvariable=self.schedule_from_var,
            state="readonly",
            style="Form.TCombobox",
        )
        self.schedule_from_combo.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(4, 2))
        self.schedule_from_combo.bind("<<ComboboxSelected>>", self._update_schedule_from_balance)
        ttk.Label(card, textvariable=self.schedule_from_balance_var, style="MonoLabel.TLabel").grid(
            row=2,
            column=0,
            columnspan=2,
            sticky="w",
        )

        ttk.Separator(card).grid(row=3, column=0, columnspan=2, sticky="ew", pady=(8, 12))

        ttk.Label(card, text="Destination Address", style="SectionHeading.TLabel").grid(row=4, column=0, columnspan=2, sticky="w")
        ttk.Entry(card, textvariable=self.schedule_dest_var, style="Form.TEntry").grid(
            row=5, column=0, columnspan=2, sticky="ew", pady=(4, 6)
        )

        ttk.Label(card, text="Amount (BLINE)", style="SectionHeading.TLabel").grid(row=6, column=0, sticky="w")
        ttk.Label(card, text="Fee Rate (BLINE/KB)", style="SectionHeading.TLabel").grid(row=6, column=1, sticky="w")
        ttk.Entry(card, textvariable=self.schedule_amount_var, style="Form.TEntry").grid(row=7, column=0, sticky="ew", pady=(4, 6))
        ttk.Entry(card, textvariable=self.schedule_fee_var, style="Form.TEntry").grid(row=7, column=1, sticky="ew", pady=(4, 6))
        ttk.Label(card, text="Scheduled Date (UTC) or block height", style="SectionHeading.TLabel").grid(
            row=8, column=0, columnspan=2, sticky="w"
        )
        ttk.Entry(card, textvariable=self.schedule_lock_var, style="Form.TEntry").grid(
            row=9, column=0, columnspan=2, sticky="ew", pady=(4, 6)
        )
        ttk.Checkbutton(
            card,
            text="Cancelable (allows refund before scheduled time)",
            variable=self.schedule_cancelable_var,
            style="ScheduledCheck.TCheckbutton",
        ).grid(row=10, column=0, columnspan=2, sticky="w", pady=(4, 4))
        ttk.Button(
            card,
            text="Schedule Payment",
            command=self._create_scheduled_transaction,
            style="Primary.TButton",
        ).grid(row=11, column=0, columnspan=2, sticky="w", pady=(8, 0))

    def _show_address_notice(self, message: str, duration: int = 2000) -> None:
        if self._address_notice_after:
            with contextlib.suppress(Exception):
                self.after_cancel(self._address_notice_after)
            self._address_notice_after = None
        if not self._address_notice_label:
            return
        if message:
            self._address_notice_var.set(message)
            if not self._address_notice_label.winfo_ismapped():
                self._address_notice_label.pack(anchor="w", pady=(6, 0))
            self._address_notice_after = self.after(duration, self._clear_address_notice)
        else:
            self._clear_address_notice()

    def _clear_address_notice(self) -> None:
        self._address_notice_after = None
        self._address_notice_var.set("")
        if self._address_notice_label and self._address_notice_label.winfo_ismapped():
            self._address_notice_label.pack_forget()

    def _copy_selected_address(self, event: tk.Event[tk.Misc] | None) -> None:
        tree = self.address_tree
        if not tree:
            return
        target = None
        if event is not None and getattr(event, "num", None) == 1:
            with contextlib.suppress(Exception):
                target = tree.identify_row(event.y)
        if target:
            tree.selection_set(target)
        selection = tree.selection()
        if not selection:
            return
        values = tree.item(selection[0], "values")
        if not values:
            return
        address = values[0]
        if not address or address == "RPC offline":
            return
        try:
            self.clipboard_clear()
            self.clipboard_append(address)
            self._show_address_notice("Address copied to clipboard.")
        except Exception as exc:
            print(f"[wallet-gui] Unable to copy address: {exc}")

    def _build_send_tab(self, frame: ttk.Frame) -> None:
        card = ttk.Frame(frame, style="Card.TFrame", padding=16)
        card.pack(fill="both", expand=True)
        card.columnconfigure(0, weight=3)
        card.columnconfigure(1, weight=2)

        ttk.Label(card, text="From Address", style="SectionHeading.TLabel").grid(
            row=0, column=0, columnspan=2, sticky="w"
        )
        self.from_combo = ttk.Combobox(card, textvariable=self.send_from_var, state="readonly", style="Form.TCombobox")
        self.from_combo.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(4, 2))
        self.from_combo.bind("<<ComboboxSelected>>", self._update_from_balance)
        ttk.Label(card, textvariable=self.send_from_balance_var, style="MonoLabel.TLabel").grid(
            row=2, column=0, columnspan=2, sticky="w"
        )

        ttk.Separator(card).grid(row=3, column=0, columnspan=2, sticky="ew", pady=(8, 12))

        ttk.Label(card, text="Destination Address", style="SectionHeading.TLabel").grid(row=4, column=0, sticky="w")
        ttk.Entry(card, textvariable=self.send_address_var, style="Form.TEntry").grid(
            row=5, column=0, columnspan=2, sticky="ew", pady=(2, 6)
        )

        ttk.Label(card, text="Amount (BLINE)", style="SectionHeading.TLabel").grid(row=6, column=0, sticky="w")
        ttk.Label(card, text="Fee Rate (BLINE/KB)", style="SectionHeading.TLabel").grid(row=6, column=1, sticky="w")
        ttk.Entry(card, textvariable=self.send_amount_var, style="Form.TEntry").grid(row=7, column=0, sticky="ew", pady=(2, 6))
        ttk.Entry(card, textvariable=self.send_fee_var, style="Form.TEntry").grid(row=7, column=1, sticky="ew", pady=(2, 6), padx=(12, 0))

        ttk.Label(card, text="Memo for yourself (optional)", style="SectionHeading.TLabel").grid(
            row=8, column=0, columnspan=2, sticky="w", pady=(6, 0)
        )
        ttk.Entry(card, textvariable=self.send_memo_var, style="Form.TEntry").grid(
            row=9, column=0, columnspan=2, sticky="ew", pady=(2, 6)
        )

        ttk.Label(card, text="Memo shown to recipient (optional)", style="SectionHeading.TLabel").grid(
            row=10, column=0, columnspan=2, sticky="w"
        )
        ttk.Entry(card, textvariable=self.send_memo_to_var, style="Form.TEntry").grid(
            row=11, column=0, columnspan=2, sticky="ew", pady=(2, 10)
        )

        ttk.Button(card, text="Send Payment", command=self._send_payment, style="Primary.TButton").grid(
            row=12, column=0, pady=(8, 0), sticky="w"
        )

    def _build_mempool_tab(self, frame: ttk.Frame) -> None:
        self.mempool_box = tk.Text(
            frame,
            height=20,
            wrap="none",
            bg=PALETTE["panel"],
            fg=PALETTE["text"],
            insertbackground=PALETTE["text"],
            highlightbackground=PALETTE["accent"],
            highlightcolor=PALETTE["accent"],
            relief="flat",
        )
        self.mempool_box.pack(fill="both", expand=True)
