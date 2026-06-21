use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use kameo::console::wire::{
    ActorId, ActorSnapshot, ActorStatus, RestartPolicy, Snapshot, SupervisorStrategy, WaitKind,
};
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Style, Stylize},
    text::{Line, Span},
    widgets::{
        Block, Cell, Clear, Padding, Paragraph, Row, Scrollbar, ScrollbarOrientation,
        ScrollbarState, Table, TableState,
    },
};

use crate::ConnectionState;

const TICK: Duration = Duration::from_millis(250);
const RATE_HISTORY: usize = 120;
const INTERVAL_STEP_MS: u64 = 100;
const INTERVAL_MIN_MS: u64 = 100;
const INTERVAL_MAX_MS: u64 = 10_000;
/// Selected-row background — a muted red, like btop's process selection. Cells set only
/// foreground colors, so this shows through without inverting them.
const SELECT_BG: Color = Color::Rgb(96, 48, 56);
/// Dimmer variant used when focus is in the inspect panel, not the list.
const SELECT_BG_DIM: Color = Color::Rgb(54, 34, 40);
/// Faint tint on the selected actor's ancestor rows, to show which group/lineage it's in.
const ANCESTOR_BG: Color = Color::Rgb(38, 30, 34);
/// A softer, btop-style green for healthy states, instead of harsh ANSI green.
const GREEN: Color = Color::Rgb(143, 196, 110);
/// Forced dark background + light foreground (like btop/lazygit), so the console looks the
/// same regardless of the terminal theme. Widgets that don't set their own bg inherit this.
const BG: Color = Color::Rgb(18, 18, 22);
const FG: Color = Color::Rgb(205, 205, 212);
/// btop-style focus fade: brightness lost per row of distance from the selection, and the
/// dimmest a row is allowed to fade to (as a fraction of its original color, toward `BG`).
const FADE_STEP: f32 = 0.02;
const FADE_FLOOR: f32 = 0.3;
/// Width (in cells) of the per-actor msg/s sparkline. Braille packs 2 samples per cell, so
/// this shows the last `SPARK_WIDTH * 2` poll samples, scrolling left as new ones arrive.
const SPARK_WIDTH: usize = 9;
/// Color of the always-present sparkline baseline where no messages flowed (btop-style dim
/// grey); cells with activity are drawn in cyan instead.
const SPARK_IDLE: Color = Color::Rgb(70, 70, 80);
const SPARK_ACTIVE: Color = Color::Rgb(110, 180, 200);
/// Minimum table width (borders included) to fit the full column set: id…restarts plus the
/// sparkline. Below this we drop to the compact columns. Checked against the table's own area,
/// so the sparkline survives an open inspect panel as long as the list column keeps this width.
const FULL_COLUMNS_MIN_WIDTH: u16 = 85;

#[derive(Clone, Copy, PartialEq, Eq)]
enum SortCol {
    Id,
    Name,
    State,
    Mailbox,
    Msgs,
    Restarts,
}

impl SortCol {
    /// Numeric columns default to descending (biggest/worst first); text columns ascending.
    fn default_desc(self) -> bool {
        matches!(
            self,
            SortCol::State | SortCol::Mailbox | SortCol::Msgs | SortCol::Restarts
        )
    }
}

pub struct App {
    addr: SocketAddr,
    /// Shared with the poller thread: the latest snapshot and connection state.
    source_snapshot: Arc<Mutex<Option<Snapshot>>>,
    source_connection: Arc<Mutex<ConnectionState>>,
    connection: ConnectionState,
    prev: Option<Snapshot>,
    snapshot: Option<Snapshot>,
    rows: Vec<ActorRow>,
    deadlocks: Vec<Vec<ActorId>>,
    rate_history: HashMap<ActorId, VecDeque<u64>>,
    sort: SortCol,
    sort_desc: bool,
    collapsed: HashSet<ActorId>,
    poll_interval: Arc<AtomicU64>,
    filter: String,
    filter_mode: bool,
    table_state: TableState,
    inspect: bool,
    panel_focus: bool,
    panel_scroll: u16,
    show_help: bool,
    exit: bool,
}

impl App {
    pub fn live(
        addr: SocketAddr,
        snapshot: Arc<Mutex<Option<Snapshot>>>,
        connection: Arc<Mutex<ConnectionState>>,
        poll_interval: Arc<AtomicU64>,
    ) -> Self {
        App {
            addr,
            source_snapshot: snapshot,
            source_connection: connection,
            connection: ConnectionState::Connecting,
            prev: None,
            snapshot: None,
            rows: Vec::new(),
            deadlocks: Vec::new(),
            rate_history: HashMap::new(),
            sort: SortCol::Id,
            sort_desc: false,
            collapsed: HashSet::new(),
            poll_interval,
            filter: String::new(),
            filter_mode: false,
            table_state: TableState::default(),
            inspect: false,
            panel_focus: false,
            panel_scroll: 0,
            show_help: false,
            exit: false,
        }
    }

    pub fn run(&mut self, terminal: &mut DefaultTerminal) -> io::Result<()> {
        while !self.exit {
            self.refresh();
            terminal.draw(|frame| self.draw(frame))?;
            self.handle_events()?;
        }
        Ok(())
    }

    fn refresh(&mut self) {
        self.connection = self.source_connection.lock().unwrap().clone();

        let latest = self.source_snapshot.lock().unwrap().clone();
        let Some(latest) = latest else {
            return;
        };

        let unchanged = self.snapshot.as_ref().is_some_and(|s| s.seq == latest.seq);
        if unchanged {
            return;
        }

        match self.snapshot.take() {
            Some(current) if latest.seq > current.seq => self.prev = Some(current),
            _ => self.prev = None,
        }
        self.snapshot = Some(latest);
        self.rebuild();

        if self.table_state.selected().is_none() && !self.rows.is_empty() {
            self.table_state.select(Some(0));
        }
        self.clamp_selection();
    }

    fn rebuild(&mut self) {
        let Some(snapshot) = &self.snapshot else {
            self.rows.clear();
            self.deadlocks.clear();
            return;
        };
        self.deadlocks = detect_deadlocks(snapshot);
        let deadlocked: HashSet<ActorId> = self.deadlocks.iter().flatten().copied().collect();

        // Filtering flattens the hierarchy; otherwise show the supervision tree.
        self.rows = if self.filter.is_empty() {
            build_tree_rows(
                snapshot,
                self.prev.as_ref(),
                &deadlocked,
                &self.collapsed,
                self.sort,
                self.sort_desc,
            )
        } else {
            build_flat_rows(
                snapshot,
                self.prev.as_ref(),
                &deadlocked,
                &self.filter,
                self.sort,
                self.sort_desc,
            )
        };

        self.update_rate_history();
        self.update_sparklines();
        self.clamp_selection();
    }

    /// Fills each row's braille msg/s sparkline. Normalizes against the busiest actor's recent
    /// peak so the graphs are directly comparable (btop-style), like CPU% across cores.
    fn update_sparklines(&mut self) {
        let App {
            rows, rate_history, ..
        } = self;
        let window = SPARK_WIDTH * 2;
        let max = rows
            .iter()
            .filter_map(|r| rate_history.get(&r.id))
            .flat_map(|h| h.iter().rev().take(window).copied())
            .max()
            .unwrap_or(0);
        for row in rows.iter_mut() {
            let samples: Vec<u64> = rate_history
                .get(&row.id)
                .map(|h| h.iter().copied().collect())
                .unwrap_or_default();
            row.spark = sparkline_line(&samples, max, SPARK_WIDTH);
        }
    }

    fn update_rate_history(&mut self) {
        let alive: HashSet<ActorId> = match &self.snapshot {
            Some(snapshot) => snapshot.actors.iter().map(|a| a.id).collect(),
            None => return,
        };
        for row in &self.rows {
            if let Some(rate) = row.msg_per_sec {
                let hist = self.rate_history.entry(row.id).or_default();
                hist.push_back(rate);
                if hist.len() > RATE_HISTORY {
                    hist.pop_front();
                }
            }
        }
        self.rate_history.retain(|id, _| alive.contains(id));
    }

    fn draw(&mut self, frame: &mut Frame) {
        let area = frame.area();

        // Paint a uniform dark background behind every panel (and the borderless search line),
        // so the console looks the same regardless of the terminal theme. Panels have
        // transparent backgrounds and show this through.
        frame.render_widget(Block::new().style(Style::new().bg(BG).fg(FG)), area);

        // Three tiled, individually bordered panels (lazygit/btop style): a header box, the
        // actor list, and — when inspecting — a side panel. The deadlock banner lives inside
        // the header box; the search line is a borderless row pinned to the bottom.
        let banner = !self.deadlocks.is_empty();
        let header_height = if banner { 5 } else { 4 }; // 2 borders + 2 status lines (+ banner)
        let mut constraints = vec![Constraint::Length(header_height), Constraint::Fill(1)];
        if self.filter_mode {
            constraints.push(Constraint::Length(1)); // search line
        }
        let areas = Layout::vertical(constraints).split(area);
        let mut next = areas.iter().copied();
        let header = next.next().unwrap();
        let body = next.next().unwrap();
        let search = self.filter_mode.then(|| next.next().unwrap());

        self.render_header(frame, header);

        // Responsive placement of the inspect panel: side-by-side when there's width to spare,
        // otherwise stacked below the list (flex-wrap style), and hidden only when neither fits.
        let panel_width = (body.width / 3).clamp(30, 46);
        let fits_beside = body.width > panel_width + 24;
        let fits_below = body.height >= 15; // ≥5 list rows + a gap + ≥8 panel rows (incl. borders)
        match self.inspect {
            true if fits_beside => {
                let [list, _gap, panel] = Layout::horizontal([
                    Constraint::Fill(1),
                    Constraint::Length(1),
                    Constraint::Length(panel_width),
                ])
                .areas(body);
                self.render_table(frame, list);
                self.render_inspect_panel(frame, panel);
            }
            true if fits_below => {
                let panel_height = (body.height * 45 / 100).clamp(8, 16);
                let [list, _gap, panel] = Layout::vertical([
                    Constraint::Fill(1),
                    Constraint::Length(1),
                    Constraint::Length(panel_height),
                ])
                .areas(body);
                self.render_table(frame, list);
                self.render_inspect_panel(frame, panel);
            }
            _ => self.render_table(frame, body),
        }

        if let Some(search) = search {
            self.render_search(frame, search);
        }

        if self.show_help {
            self.render_help(frame);
        }
    }

    fn render_help(&self, frame: &mut Frame) {
        let area = frame.area();
        let lines = help_lines();
        let popup = centered_rect(area, 54, lines.len() as u16 + 2);

        // Dim the UI behind the popup, then clear the popup's own cells so nothing bleeds through.
        frame.buffer_mut().set_style(area, Style::new().dim());
        frame.render_widget(Clear, popup);

        let block = Block::bordered()
            .border_style(Style::new().cyan())
            .style(Style::new().bg(BG).fg(FG))
            .padding(Padding::horizontal(1))
            .title_top(Line::from(" Keybindings ").bold().cyan())
            .title_bottom(
                Line::from(" press ? or Esc to close ")
                    .right_aligned()
                    .dim(),
            );
        frame.render_widget(Paragraph::new(lines).block(block), popup);
    }

    fn render_search(&self, frame: &mut Frame, area: Rect) {
        // Inset to roughly align with the bordered panels' content.
        let area = Rect {
            x: area.x + 2,
            width: area.width.saturating_sub(2),
            ..area
        };
        let line = Line::from(vec![
            Span::styled("/", Style::new().red().bold()),
            Span::raw(self.filter.clone()),
            Span::styled("▏", Style::new().dim()),
        ]);
        frame.render_widget(line, area);
    }

    fn render_header(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .border_style(Style::new().fg(FG))
            .padding(Padding::horizontal(1))
            .title_top(Line::from(" Kameo Console ").left_aligned().bold())
            .title_top(
                Line::from(concat!(" ", env!("CARGO_PKG_VERSION"), " "))
                    .right_aligned()
                    .dim(),
            );
        let inner = block.inner(area);
        frame.render_widget(block, area);

        // Two status lines, plus a deadlock banner line when there are deadlocks.
        let banner = !self.deadlocks.is_empty();
        let mut rows = vec![Constraint::Length(1), Constraint::Length(1)];
        if banner {
            rows.push(Constraint::Length(1));
        }
        let parts = Layout::vertical(rows).split(inner);
        let line1 = parts[0];
        let line2 = parts[1];

        // Line 1: actor status breakdown (left) + connection (right).
        let snap = self.snapshot.as_ref();
        let total = snap.map_or(0, |s| s.actors.len());
        let mut running = 0;
        let mut restarting = 0;
        let mut dead = 0;
        let mut queued = 0usize;
        let mut rate_total = 0u64;
        if let Some(s) = snap {
            let (prev_received, dt) = rate_context(s, self.prev.as_ref());
            for a in &s.actors {
                match a.status {
                    ActorStatus::Running => running += 1,
                    ActorStatus::Restarting => restarting += 1,
                    ActorStatus::Stopped { .. } => dead += 1,
                    _ => {}
                }
                queued += a.mailbox.len;
                rate_total += actor_rate(a, &prev_received, dt);
            }
        }

        let counts = Line::from(vec![
            Span::raw(format!("Actors {total}")),
            Span::styled("  ·  ", Style::new().dim()),
            Span::styled(format!("Running {running}"), Style::new().fg(GREEN)),
            Span::styled("  ·  ", Style::new().dim()),
            Span::styled(
                format!("Restarting {restarting}"),
                if restarting > 0 {
                    Style::new().yellow()
                } else {
                    Style::new().dim()
                },
            ),
            Span::styled("  ·  ", Style::new().dim()),
            Span::styled(
                format!("Dead {dead}"),
                if dead > 0 {
                    Style::new().red()
                } else {
                    Style::new().dim()
                },
            ),
        ]);

        let conn = match &self.connection {
            ConnectionState::Connected => {
                Span::styled(format!("● {}", self.addr), Style::new().fg(GREEN))
            }
            ConnectionState::Connecting => Span::styled(
                format!("◌ connecting {}…", self.addr),
                Style::new().yellow(),
            ),
            ConnectionState::Disconnected { error, .. } => {
                Span::styled(format!("✗ {error}"), Style::new().red())
            }
        };

        // Line 2: throughput + queue (left) + uptime & poll interval (right).
        let uptime = snap.map(|s| s.uptime).unwrap_or_default();
        let interval = self.poll_interval.load(AtomicOrdering::Relaxed);
        let stats = Line::from(vec![
            Span::styled(format!("{rate_total} msg/s"), Style::new().cyan()),
            Span::styled("  ·  ", Style::new().dim()),
            Span::raw(format!("{queued} queued")),
        ]);
        let meta = Line::from(format!("up {} · {interval}ms", fmt_uptime(uptime)))
            .right_aligned()
            .dim();

        let [l1, r1] =
            Layout::horizontal([Constraint::Fill(1), Constraint::Length(conn.width() as u16)])
                .areas(line1);
        frame.render_widget(counts, l1);
        frame.render_widget(Line::from(conn).right_aligned(), r1);
        frame.render_widget(stats, line2);
        frame.render_widget(meta, line2);

        if banner {
            self.render_deadlock_banner(frame, parts[2]);
        }
    }

    fn render_deadlock_banner(&self, frame: &mut Frame, area: Rect) {
        let Some(snapshot) = &self.snapshot else {
            return;
        };
        let names: HashMap<ActorId, &str> = snapshot
            .actors
            .iter()
            .map(|a| (a.id, a.name.as_str()))
            .collect();
        let mut text = format!("⚠ DEADLOCK  {}", fmt_cycle(&self.deadlocks[0], &names));
        if self.deadlocks.len() > 1 {
            text.push_str(&format!("   (+{} more)", self.deadlocks.len() - 1));
        }
        frame.render_widget(Line::from(text).style(Style::new().red().bold()), area);
    }

    fn render_table(&mut self, frame: &mut Frame, area: Rect) {
        // Drop the wider columns (msg/s, sparkline, restarts) once the list is too narrow to
        // hold them. Keyed off the table's own width, so an open inspect panel only forces
        // compact mode when it actually squeezes the list below the full-column threshold.
        let compact = area.width < FULL_COLUMNS_MIN_WIDTH;
        // The list owns focus whenever the inspect panel doesn't; light its border to match.
        let accent = panel_accent(!self.panel_focus);
        // Position of the selection within the visible list, e.g. "7/413".
        let pos = self.table_state.selected().map_or(0, |i| i + 1);
        let block = Block::bordered()
            .border_style(accent)
            .padding(Padding::horizontal(1))
            .title_top(Line::from(" Actors ").style(accent).bold())
            .title_bottom(Line::from(format!(" {pos}/{} ", self.rows.len())).dim())
            .title_bottom(Line::from(" ? help ").right_aligned().dim());
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let area = inner;

        let (col, desc) = (self.sort, self.sort_desc);
        let h = |label, key_pos, c, right_align| {
            sortable_header(label, key_pos, c, col, desc, right_align)
        };
        let (header, widths): (Row, Vec<Constraint>) = if compact {
            (
                Row::new([
                    h("id", 0, SortCol::Id, false),
                    h("name", 0, SortCol::Name, false),
                    h("state", 0, SortCol::State, false),
                    h("mailbox", 0, SortCol::Mailbox, true),
                    Cell::from(""),
                ]),
                vec![
                    Constraint::Length(9),
                    Constraint::Min(14),
                    Constraint::Length(13),
                    Constraint::Length(8),
                    Constraint::Length(2),
                ],
            )
        } else {
            (
                Row::new([
                    h("id", 0, SortCol::Id, false),
                    h("name", 0, SortCol::Name, false),
                    h("state", 0, SortCol::State, false),
                    h("mailbox", 0, SortCol::Mailbox, true),
                    h("msg/s", 2, SortCol::Msgs, true),
                    Cell::from(""),
                    h("restarts", 0, SortCol::Restarts, true),
                    Cell::from(""),
                ]),
                vec![
                    Constraint::Length(9),
                    Constraint::Min(18),
                    Constraint::Length(13),
                    Constraint::Length(8),
                    Constraint::Length(6),
                    Constraint::Length(SPARK_WIDTH as u16),
                    Constraint::Length(9),
                    Constraint::Length(2),
                ],
            )
        };

        // The header takes one row; the rest are data rows. Reserve a right gutter for a
        // scrollbar when the list is taller than the viewport.
        let visible = area.height.saturating_sub(1) as usize;
        let overflow = self.rows.len() > visible;
        let (table_area, scrollbar_area) = if overflow {
            let [table_area, gutter] =
                Layout::horizontal([Constraint::Fill(1), Constraint::Length(1)]).areas(area);
            (table_area, Some(gutter))
        } else {
            (area, None)
        };

        // Background-only highlight: keeps each cell's fg color (green/red/…) intact, unlike
        // `reversed()` which turned colored cells into solid colored backgrounds. Dimmer when
        // focus is in the inspect panel.
        let highlight = Style::new().bg(if self.panel_focus {
            SELECT_BG_DIM
        } else {
            SELECT_BG
        });
        let ancestors = self.ancestor_ids();
        let rows: Vec<Row> = self
            .rows
            .iter()
            .map(|r| {
                let row = r.to_row(compact);
                if ancestors.contains(&r.id) {
                    row.style(Style::new().bg(ANCESTOR_BG))
                } else {
                    row
                }
            })
            .collect();
        let table = Table::new(rows, widths)
            .header(header)
            .column_spacing(1)
            .row_highlight_style(highlight);

        frame.render_stateful_widget(table, table_area, &mut self.table_state);

        // btop-style focus fade: rows dim toward the background the further they sit from the
        // selection, drawing the eye to the current actor. A buffer pass is the only way to
        // fade every cell color uniformly (state dots, mailbox, flags), not just plain text.
        if let Some(selected) = self.table_state.selected() {
            let offset = self.table_state.offset();
            let data_top = table_area.top() + 1; // skip the header row
            let buf = frame.buffer_mut();
            for y in data_top..table_area.bottom() {
                let idx = offset + (y - data_top) as usize;
                if idx >= self.rows.len() {
                    break;
                }
                let factor = (1.0 - idx.abs_diff(selected) as f32 * FADE_STEP).max(FADE_FLOOR);
                if factor >= 1.0 {
                    continue; // the selected row keeps full brightness
                }
                for x in table_area.left()..table_area.right() {
                    let cell = &mut buf[(x, y)];
                    cell.set_fg(fade_toward_bg(cell.fg, factor));
                }
            }
        }

        if let Some(mut gutter) = scrollbar_area {
            // Align the scrollbar track with the data rows (below the header).
            gutter.y = gutter.y.saturating_add(1);
            gutter.height = gutter.height.saturating_sub(1);
            let mut state = ScrollbarState::new(self.rows.len())
                .position(self.table_state.selected().unwrap_or(0));
            frame.render_stateful_widget(
                Scrollbar::new(ScrollbarOrientation::VerticalRight)
                    .begin_symbol(None)
                    .end_symbol(None),
                gutter,
                &mut state,
            );
        }
    }

    fn render_inspect_panel(&mut self, frame: &mut Frame, area: Rect) {
        let (title, lines) = match self.selected_actor() {
            Some((actor, row)) => {
                let snapshot = self.snapshot.as_ref().unwrap();
                let history = self.rate_history.get(&actor.id);
                let title = format!(" #{:03} {} ", actor.id.0, short_type_name(&actor.name));
                (
                    title,
                    build_panel_lines(actor, row, snapshot, &self.deadlocks, history),
                )
            }
            None => (
                " Inspect ".to_string(),
                vec![Line::from("no actor selected").dim()],
            ),
        };

        let accent = panel_accent(self.panel_focus);
        let block = Block::bordered()
            .border_style(accent)
            .padding(Padding::horizontal(1))
            .title_top(Line::from(title).style(accent).bold());
        let inner = block.inner(area);
        frame.render_widget(block, area);

        // One row per line (no wrap), so the height is exact for scroll bounds. Reserve a
        // 1-col scrollbar gutter on the right.
        let [text_area, gutter] =
            Layout::horizontal([Constraint::Fill(1), Constraint::Length(1)]).areas(inner);
        let content_height = lines.len() as u16;
        let visible = text_area.height;
        let max_scroll = content_height.saturating_sub(visible);
        self.panel_scroll = self.panel_scroll.min(max_scroll);

        frame.render_widget(
            Paragraph::new(lines).scroll((self.panel_scroll, 0)),
            text_area,
        );

        if max_scroll > 0 {
            // Size the bar to the scroll range so `panel_scroll == max_scroll` reaches the
            // bottom (the scroll offset spans 0..=max_scroll, not 0..content_height).
            let mut state =
                ScrollbarState::new(max_scroll as usize + 1).position(self.panel_scroll as usize);
            let style = if self.panel_focus {
                Style::new().cyan()
            } else {
                Style::new().dim()
            };
            frame.render_stateful_widget(
                Scrollbar::new(ScrollbarOrientation::VerticalRight)
                    .begin_symbol(None)
                    .end_symbol(None)
                    .style(style),
                gutter,
                &mut state,
            );
        }
    }

    /// The ancestor (parent, grandparent, …) ids of the selected actor, for lineage highlighting.
    fn ancestor_ids(&self) -> HashSet<ActorId> {
        let mut set = HashSet::new();
        let (Some(snapshot), Some(row)) = (
            self.snapshot.as_ref(),
            self.table_state.selected().and_then(|i| self.rows.get(i)),
        ) else {
            return set;
        };
        let parents: HashMap<ActorId, ActorId> = snapshot
            .actors
            .iter()
            .filter_map(|a| a.links.parent.map(|p| (a.id, p)))
            .collect();
        let mut cur = row.id;
        while let Some(&parent) = parents.get(&cur) {
            if !set.insert(parent) {
                break; // guard against a malformed cycle
            }
            cur = parent;
        }
        set
    }

    fn selected_actor(&self) -> Option<(&ActorSnapshot, &ActorRow)> {
        let snapshot = self.snapshot.as_ref()?;
        let row = self.rows.get(self.table_state.selected()?)?;
        let actor = snapshot.actors.iter().find(|a| a.id == row.id)?;
        Some((actor, row))
    }

    fn handle_events(&mut self) -> io::Result<()> {
        if event::poll(TICK)?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            self.on_key(key.code, key.modifiers);
        }
        Ok(())
    }

    fn on_key(&mut self, code: KeyCode, modifiers: KeyModifiers) {
        // The help popup is modal: it swallows every key except the ones that dismiss it.
        if self.show_help {
            match code {
                KeyCode::Char('c') if modifiers.contains(KeyModifiers::CONTROL) => self.exit = true,
                KeyCode::Char('?' | 'q') | KeyCode::Esc | KeyCode::Enter => self.show_help = false,
                _ => {}
            }
            return;
        }

        if self.filter_mode {
            match code {
                KeyCode::Char(c) => {
                    self.filter.push(c);
                    self.rebuild();
                }
                KeyCode::Backspace if self.filter.is_empty() => {
                    self.filter_mode = false;
                }
                KeyCode::Backspace => {
                    self.filter.pop();
                    self.rebuild();
                }
                KeyCode::Enter => self.filter_mode = false,
                KeyCode::Esc => {
                    self.filter_mode = false;
                    self.filter.clear();
                    self.rebuild();
                }
                _ => {}
            }
            return;
        }

        // When the inspect panel is focused, navigation scrolls it instead of the list.
        if self.inspect && self.panel_focus {
            match code {
                KeyCode::Char('q') => self.exit = true,
                KeyCode::Char('c') if modifiers.contains(KeyModifiers::CONTROL) => self.exit = true,
                KeyCode::Tab | KeyCode::BackTab => self.panel_focus = false,
                KeyCode::Esc | KeyCode::Enter => self.close_inspect(),
                KeyCode::Down | KeyCode::Char('j') => {
                    self.panel_scroll = self.panel_scroll.saturating_add(1)
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    self.panel_scroll = self.panel_scroll.saturating_sub(1)
                }
                KeyCode::PageDown => self.panel_scroll = self.panel_scroll.saturating_add(10),
                KeyCode::PageUp => self.panel_scroll = self.panel_scroll.saturating_sub(10),
                KeyCode::Home => self.panel_scroll = 0,
                KeyCode::End => self.panel_scroll = u16::MAX, // clamped to content in render
                _ => {}
            }
            return;
        }

        match code {
            KeyCode::Char('q') => self.exit = true,
            KeyCode::Char('c') if modifiers.contains(KeyModifiers::CONTROL) => self.exit = true,
            KeyCode::Esc if self.inspect => self.close_inspect(),
            KeyCode::Esc if !self.filter.is_empty() => {
                self.filter.clear();
                self.rebuild();
            }
            KeyCode::Esc => self.exit = true,
            KeyCode::Enter => {
                if self.inspect {
                    self.close_inspect();
                } else {
                    self.inspect = true;
                }
            }
            KeyCode::Tab | KeyCode::BackTab => {
                if self.inspect {
                    self.panel_focus = true;
                } else {
                    self.inspect = true;
                }
            }
            KeyCode::Char('?') => self.show_help = true,
            KeyCode::Char('/') => self.filter_mode = true,
            KeyCode::Char('i') => self.set_sort(SortCol::Id),
            KeyCode::Char('n') => self.set_sort(SortCol::Name),
            KeyCode::Char('s') => self.set_sort(SortCol::State),
            KeyCode::Char('m') => self.set_sort(SortCol::Mailbox),
            KeyCode::Char('g') => self.set_sort(SortCol::Msgs),
            KeyCode::Char('r') => self.set_sort(SortCol::Restarts),
            KeyCode::Char(' ') => self.toggle_collapsed(),
            KeyCode::Char('c') => self.collapse_all(),
            KeyCode::Char('e') => self.expand_all(),
            KeyCode::Left | KeyCode::Char('h') => self.set_collapsed(true),
            KeyCode::Right | KeyCode::Char('l') => self.set_collapsed(false),
            KeyCode::Char('-') => self.adjust_interval(false),
            KeyCode::Char('+') => self.adjust_interval(true),
            KeyCode::Down | KeyCode::Char('j') => self.select_next(),
            KeyCode::Up | KeyCode::Char('k') => self.select_previous(),
            KeyCode::Home => {
                self.table_state.select(Some(0));
                self.panel_scroll = 0;
            }
            KeyCode::End => {
                self.table_state
                    .select(Some(self.rows.len().saturating_sub(1)));
                self.panel_scroll = 0;
            }
            _ => {}
        }
    }

    fn set_sort(&mut self, col: SortCol) {
        if self.sort == col {
            self.sort_desc = !self.sort_desc;
        } else {
            self.sort = col;
            self.sort_desc = col.default_desc();
        }
        self.rebuild();
    }

    fn adjust_interval(&mut self, increase: bool) {
        let cur = self.poll_interval.load(AtomicOrdering::Relaxed);
        let next = if increase {
            (cur + INTERVAL_STEP_MS).min(INTERVAL_MAX_MS)
        } else {
            cur.saturating_sub(INTERVAL_STEP_MS).max(INTERVAL_MIN_MS)
        };
        self.poll_interval.store(next, AtomicOrdering::Relaxed);
    }

    fn set_collapsed(&mut self, collapsed: bool) {
        let Some(row) = self.table_state.selected().and_then(|i| self.rows.get(i)) else {
            return;
        };
        if !row.expandable {
            return;
        }
        let id = row.id;
        if collapsed {
            self.collapsed.insert(id);
        } else {
            self.collapsed.remove(&id);
        }
        self.rebuild();
    }

    fn toggle_collapsed(&mut self) {
        let collapse = self
            .table_state
            .selected()
            .and_then(|i| self.rows.get(i))
            .is_some_and(|r| r.expandable && !self.collapsed.contains(&r.id));
        self.set_collapsed(collapse);
    }

    fn collapse_all(&mut self) {
        if let Some(snapshot) = &self.snapshot {
            self.collapsed = snapshot
                .actors
                .iter()
                .filter(|a| !a.links.children.is_empty())
                .map(|a| a.id)
                .collect();
        }
        self.rebuild();
    }

    fn expand_all(&mut self) {
        self.collapsed.clear();
        self.rebuild();
    }

    fn close_inspect(&mut self) {
        self.inspect = false;
        self.panel_focus = false;
        self.panel_scroll = 0;
    }

    fn select_next(&mut self) {
        if self.rows.is_empty() {
            return;
        }
        let next = self
            .table_state
            .selected()
            .map_or(0, |i| (i + 1).min(self.rows.len() - 1));
        self.table_state.select(Some(next));
        self.panel_scroll = 0;
    }

    fn select_previous(&mut self) {
        let previous = self
            .table_state
            .selected()
            .map_or(0, |i| i.saturating_sub(1));
        self.table_state.select(Some(previous));
        self.panel_scroll = 0;
    }

    fn clamp_selection(&mut self) {
        match self.table_state.selected() {
            _ if self.rows.is_empty() => self.table_state.select(None),
            Some(i) if i >= self.rows.len() => {
                self.table_state.select(Some(self.rows.len() - 1));
            }
            _ => {}
        }
    }
}

const BUSY_THRESHOLD: Duration = Duration::from_millis(500);
const STUCK_THRESHOLD: Duration = Duration::from_secs(5);

#[derive(Clone, Copy)]
struct MailboxView {
    len: usize,
    capacity: Option<usize>,
}

struct ActorRow {
    id: ActorId,
    indent: String,
    marker: &'static str,
    id_label: String,
    name: String,
    status: ActorStatus,
    handling: Option<(String, Duration)>,
    mailbox: Option<MailboxView>,
    msg_per_sec: Option<u64>,
    /// Braille sparkline of recent msg/s, filled in by `App::update_sparklines` after the
    /// rate history is updated (it needs the cross-actor max for normalization).
    spark: Line<'static>,
    restarts: u64,
    deadlocked: bool,
    expandable: bool,
    /// Byte range within `name` matching the active filter, to highlight.
    name_match: Option<(usize, usize)>,
    flag: Option<Flag>,
}

#[derive(Clone, Copy)]
enum Flag {
    Deadlock,
    Stuck,
    Restart,
    Dead,
    Linked,
}

impl ActorRow {
    fn new(
        actor: &ActorSnapshot,
        indent: String,
        marker: &'static str,
        expandable: bool,
        prev_received: &HashMap<ActorId, u64>,
        dt: Option<Duration>,
        deadlocked: bool,
    ) -> Self {
        let running = matches!(actor.status, ActorStatus::Running);
        let mailbox = running.then_some(MailboxView {
            len: actor.mailbox.len,
            capacity: actor.mailbox.capacity,
        });
        let handling = actor
            .handling
            .as_ref()
            .map(|h| (h.message.clone(), h.elapsed));

        let msg_per_sec = match (running, dt, prev_received.get(&actor.id)) {
            (true, Some(dt), Some(&prev)) if dt.as_secs_f64() > 0.0 => {
                let delta = actor.counters.messages_received.saturating_sub(prev);
                Some((delta as f64 / dt.as_secs_f64()).round() as u64)
            }
            _ => None,
        };

        let stuck = handling
            .as_ref()
            .is_some_and(|(_, e)| *e >= STUCK_THRESHOLD);
        let flag = if matches!(actor.status, ActorStatus::Stopped { .. }) {
            Some(Flag::Dead)
        } else if deadlocked {
            Some(Flag::Deadlock)
        } else if running && stuck {
            Some(Flag::Stuck)
        } else if matches!(actor.status, ActorStatus::Restarting) || actor.counters.restarts > 0 {
            Some(Flag::Restart)
        } else if !actor.links.siblings.is_empty() {
            Some(Flag::Linked)
        } else {
            None
        };

        ActorRow {
            id: actor.id,
            indent,
            marker,
            id_label: format!("#{:03}", actor.id.0),
            name: short_type_name(&actor.name).to_string(),
            status: actor.status.clone(),
            handling,
            mailbox,
            msg_per_sec,
            spark: Line::default(),
            restarts: actor.counters.restarts,
            deadlocked,
            expandable,
            name_match: None,
            flag,
        }
    }

    fn state_cell(&self) -> (String, Style) {
        if self.deadlocked {
            return ("Deadlock".into(), Style::new().red().bold());
        }
        match &self.status {
            ActorStatus::Stopped { .. } => ("Dead".into(), Style::new().red().dim()),
            ActorStatus::Restarting => ("Restarting".into(), Style::new().yellow()),
            ActorStatus::Starting => ("Starting".into(), Style::new().dim()),
            ActorStatus::Stopping => ("Stopping".into(), Style::new().dim()),
            ActorStatus::Running => match &self.handling {
                Some((_, e)) if *e >= STUCK_THRESHOLD => (
                    format!("Stuck {}", fmt_short(*e)),
                    Style::new().red().bold(),
                ),
                Some((_, e)) if *e >= BUSY_THRESHOLD => {
                    (format!("Busy {}", fmt_short(*e)), Style::new().yellow())
                }
                _ => ("Running".into(), Style::new().fg(GREEN)),
            },
        }
    }

    fn mailbox_cell(&self) -> Line<'static> {
        match self.mailbox {
            None => right("—".into()).dim(),
            Some(MailboxView {
                len,
                capacity: Some(cap),
            }) => Line::from(format!("{len}/{cap}"))
                .right_aligned()
                .style(backpressure_style(len, cap)),
            Some(MailboxView {
                len,
                capacity: None,
            }) => Line::from(format!("{len}/∞")).right_aligned().fg(FG),
        }
    }

    fn name_cell(&self) -> Cell<'static> {
        let prefix = format!("{}{}", self.indent, self.marker);
        match self.name_match {
            // The offsets come from a lowercased copy of the name; guard against char boundaries
            // so a name that changes byte length when lowercased can't slice mid-codepoint (panic).
            Some((s, e))
                if s < e
                    && e <= self.name.len()
                    && self.name.is_char_boundary(s)
                    && self.name.is_char_boundary(e) =>
            {
                Cell::from(Line::from(vec![
                    Span::raw(format!("{prefix}{}", &self.name[..s])),
                    Span::styled(
                        self.name[s..e].to_string(),
                        Style::new().black().on_yellow(),
                    ),
                    Span::raw(self.name[e..].to_string()),
                ]))
            }
            _ => Cell::from(format!("{prefix}{}", self.name)),
        }
    }

    fn to_row(&self, compact: bool) -> Row<'static> {
        let (state_text, state_style) = self.state_cell();
        let (flag_text, flag_style) = self.flag.map(flag_label).unwrap_or_default();
        let id = Cell::from(self.id_label.clone());
        let name = self.name_cell();
        // A leading colored dot carries the state; the text stays neutral (lazygit-style).
        let state = Cell::from(Line::from(vec![
            Span::styled("●", state_style),
            Span::raw(format!(" {state_text}")),
        ]));
        let mailbox = Cell::from(self.mailbox_cell());
        let flag = Cell::from(Line::from(flag_text).style(flag_style));

        if compact {
            Row::new([id, name, state, mailbox, flag])
        } else {
            let spark = Cell::from(self.spark.clone());
            Row::new([
                id,
                name,
                state,
                mailbox,
                Cell::from(right(dash_or(self.msg_per_sec))),
                spark,
                Cell::from(right(self.restarts.to_string())),
                flag,
            ])
        }
    }
}

/// Finds deadlock cycles in the wait-for graph. Each actor waits on at most one other (it can
/// only be blocked in one handler), so the graph is functional and a deadlock is simply a
/// cycle — found by following each chain until it repeats or ends.
fn detect_deadlocks(snapshot: &Snapshot) -> Vec<Vec<ActorId>> {
    let next: HashMap<ActorId, ActorId> = snapshot
        .actors
        .iter()
        .filter_map(|a| a.waiting_on.as_ref().map(|w| (a.id, w.target)))
        .collect();

    let mut cycles = Vec::new();
    let mut in_cycle: HashSet<ActorId> = HashSet::new();

    for &start in next.keys() {
        if in_cycle.contains(&start) {
            continue;
        }
        let mut path = Vec::new();
        let mut pos: HashMap<ActorId, usize> = HashMap::new();
        let mut cur = start;
        loop {
            if let Some(&i) = pos.get(&cur) {
                let cycle = path[i..].to_vec();
                in_cycle.extend(cycle.iter().copied());
                cycles.push(cycle);
                break;
            }
            if in_cycle.contains(&cur) {
                break;
            }
            match next.get(&cur) {
                Some(&n) => {
                    pos.insert(cur, path.len());
                    path.push(cur);
                    cur = n;
                }
                None => break,
            }
        }
    }

    // `next` is a HashMap, so cycles are discovered in arbitrary order and rotation. Normalize
    // each to start at its lowest id (and order the cycles) so the banner doesn't flicker.
    for cycle in &mut cycles {
        if let Some(min_pos) = (0..cycle.len()).min_by_key(|&i| cycle[i].0) {
            cycle.rotate_left(min_pos);
        }
    }
    cycles.sort_by_key(|cycle| cycle.first().map(|id| id.0));
    cycles
}

/// Shared rate inputs: previous per-actor received counts and the time delta between snapshots.
fn rate_context(
    snapshot: &Snapshot,
    prev: Option<&Snapshot>,
) -> (HashMap<ActorId, u64>, Option<Duration>) {
    let prev_received = prev
        .map(|p| {
            p.actors
                .iter()
                .map(|a| (a.id, a.counters.messages_received))
                .collect()
        })
        .unwrap_or_default();
    let dt = prev.and_then(|p| snapshot.captured_at.duration_since(p.captured_at).ok());
    (prev_received, dt)
}

/// Sorts a slice of actors by the active column/direction (id as a stable tiebreak).
fn sort_actors(
    actors: &mut [&ActorSnapshot],
    sort: SortCol,
    desc: bool,
    prev_received: &HashMap<ActorId, u64>,
    dt: Option<Duration>,
) {
    actors.sort_by(|a, b| {
        let ord = compare(a, b, sort, prev_received, dt);
        if desc { ord.reverse() } else { ord }
    });
}

fn compare(
    a: &ActorSnapshot,
    b: &ActorSnapshot,
    sort: SortCol,
    prev_received: &HashMap<ActorId, u64>,
    dt: Option<Duration>,
) -> Ordering {
    let tie = a.id.0.cmp(&b.id.0);
    match sort {
        SortCol::Id => a.id.0.cmp(&b.id.0),
        SortCol::Name => short_type_name(&a.name)
            .cmp(short_type_name(&b.name))
            .then(tie),
        SortCol::State => severity(a).cmp(&severity(b)).then(tie),
        SortCol::Mailbox => a.mailbox.len.cmp(&b.mailbox.len).then(tie),
        SortCol::Msgs => actor_rate(a, prev_received, dt)
            .cmp(&actor_rate(b, prev_received, dt))
            .then(tie),
        SortCol::Restarts => a.counters.restarts.cmp(&b.counters.restarts).then(tie),
    }
}

/// Higher = more attention-worthy, so a descending State sort surfaces problems first.
fn severity(actor: &ActorSnapshot) -> u8 {
    match &actor.status {
        ActorStatus::Stopped { .. } => 5,
        ActorStatus::Restarting => 4,
        ActorStatus::Running
            if actor
                .handling
                .as_ref()
                .is_some_and(|h| h.elapsed >= STUCK_THRESHOLD) =>
        {
            4
        }
        ActorStatus::Stopping => 3,
        ActorStatus::Starting => 2,
        ActorStatus::Running => 1,
    }
}

fn actor_rate(
    actor: &ActorSnapshot,
    prev_received: &HashMap<ActorId, u64>,
    dt: Option<Duration>,
) -> u64 {
    match (dt, prev_received.get(&actor.id)) {
        (Some(dt), Some(&prev)) if dt.as_secs_f64() > 0.0 => {
            let delta = actor.counters.messages_received.saturating_sub(prev);
            (delta as f64 / dt.as_secs_f64()).round() as u64
        }
        _ => 0,
    }
}

/// A flat, optionally name-filtered and sorted list (used in list view or while filtering).
fn build_flat_rows(
    snapshot: &Snapshot,
    prev: Option<&Snapshot>,
    deadlocked: &HashSet<ActorId>,
    filter: &str,
    sort: SortCol,
    desc: bool,
) -> Vec<ActorRow> {
    let (prev_received, dt) = rate_context(snapshot, prev);
    let needle = filter.to_lowercase();
    let mut actors: Vec<&ActorSnapshot> = snapshot
        .actors
        .iter()
        .filter(|a| needle.is_empty() || a.name.to_lowercase().contains(&needle))
        .collect();
    sort_actors(&mut actors, sort, desc, &prev_received, dt);

    actors
        .into_iter()
        .map(|a| {
            let mut row = ActorRow::new(
                a,
                String::new(),
                "",
                false,
                &prev_received,
                dt,
                deadlocked.contains(&a.id),
            );
            // Highlight the matched substring, but only when it falls within the (short)
            // displayed name — a match purely in the module path has nothing to highlight.
            if !needle.is_empty()
                && let Some(start) = row.name.to_lowercase().find(&needle)
            {
                row.name_match = Some((start, start + needle.len()));
            }
            row
        })
        .collect()
}

/// The supervision tree, with siblings sorted by the active column and collapsed nodes pruned.
fn build_tree_rows(
    snapshot: &Snapshot,
    prev: Option<&Snapshot>,
    deadlocked: &HashSet<ActorId>,
    collapsed: &HashSet<ActorId>,
    sort: SortCol,
    desc: bool,
) -> Vec<ActorRow> {
    let by_id: HashMap<ActorId, &ActorSnapshot> =
        snapshot.actors.iter().map(|a| (a.id, a)).collect();
    let (prev_received, dt) = rate_context(snapshot, prev);

    let mut roots: Vec<&ActorSnapshot> = snapshot
        .actors
        .iter()
        .filter(|a| a.links.parent.is_none())
        .collect();
    sort_actors(&mut roots, sort, desc, &prev_received, dt);

    let mut rows = Vec::new();
    let count = roots.len();
    for (i, root) in roots.into_iter().enumerate() {
        push_node(
            root,
            "",
            true,
            i == count - 1,
            &by_id,
            &prev_received,
            dt,
            deadlocked,
            collapsed,
            sort,
            desc,
            &mut rows,
        );
    }
    rows
}

#[allow(clippy::too_many_arguments)]
fn push_node(
    actor: &ActorSnapshot,
    parent_prefix: &str,
    is_root: bool,
    is_last: bool,
    by_id: &HashMap<ActorId, &ActorSnapshot>,
    prev_received: &HashMap<ActorId, u64>,
    dt: Option<Duration>,
    deadlocked: &HashSet<ActorId>,
    collapsed: &HashSet<ActorId>,
    sort: SortCol,
    desc: bool,
    rows: &mut Vec<ActorRow>,
) {
    let indent = if is_root {
        String::new()
    } else if is_last {
        format!("{parent_prefix}└─")
    } else {
        format!("{parent_prefix}├─")
    };

    let mut children: Vec<&ActorSnapshot> = actor
        .links
        .children
        .iter()
        .filter_map(|id| by_id.get(id).copied())
        .collect();
    let has_children = !children.is_empty();
    let is_collapsed = collapsed.contains(&actor.id);
    let marker = if !has_children {
        "  "
    } else if is_collapsed {
        "▸ "
    } else {
        "▾ "
    };

    rows.push(ActorRow::new(
        actor,
        indent,
        marker,
        has_children,
        prev_received,
        dt,
        deadlocked.contains(&actor.id),
    ));

    if !has_children || is_collapsed {
        return;
    }
    sort_actors(&mut children, sort, desc, prev_received, dt);

    let child_prefix = if is_root {
        String::new()
    } else if is_last {
        format!("{parent_prefix}  ")
    } else {
        format!("{parent_prefix}│ ")
    };
    let count = children.len();
    for (i, child) in children.into_iter().enumerate() {
        push_node(
            child,
            &child_prefix,
            false,
            i == count - 1,
            by_id,
            prev_received,
            dt,
            deadlocked,
            collapsed,
            sort,
            desc,
            rows,
        );
    }
}

fn strategy_label(strategy: SupervisorStrategy) -> &'static str {
    match strategy {
        SupervisorStrategy::OneForOne => "OneForOne",
        SupervisorStrategy::OneForAll => "OneForAll",
        SupervisorStrategy::RestForOne => "RestForOne",
    }
}

fn status_word(status: &ActorStatus) -> &'static str {
    match status {
        ActorStatus::Starting => "Starting",
        ActorStatus::Running => "Running",
        ActorStatus::Restarting => "Restarting",
        ActorStatus::Stopping => "Stopping",
        ActorStatus::Stopped { .. } => "Dead",
    }
}

/// A column header with its sort-key letter in red, the active column bold with a `▾`/`▴`.
fn sortable_header(
    label: &'static str,
    key_pos: usize,
    col: SortCol,
    active: SortCol,
    desc: bool,
    right_align: bool,
) -> Cell<'static> {
    let is_active = col == active;
    let mut spans: Vec<Span> = label
        .chars()
        .enumerate()
        .map(|(i, ch)| {
            let style = if i == key_pos {
                Style::new().red().bold()
            } else if is_active {
                Style::new().bold()
            } else {
                Style::new().dim()
            };
            Span::styled(ch.to_string(), style)
        })
        .collect();
    if is_active {
        // No leading space: right-aligned headers are width-tight and would clip the key letter.
        spans.push(Span::styled(
            if desc { "▾" } else { "▴" },
            Style::new().bold(),
        ));
    }
    let line = Line::from(spans);
    Cell::from(if right_align {
        line.right_aligned()
    } else {
        line
    })
}

fn flag_label(flag: Flag) -> (&'static str, Style) {
    match flag {
        Flag::Deadlock => ("🔒", Style::new().red().bold()),
        Flag::Stuck => ("⏳", Style::new().red()),
        Flag::Restart => ("⚠", Style::new().yellow()),
        Flag::Dead => ("✗", Style::new().red()),
        Flag::Linked => ("↔", Style::new().cyan()),
    }
}

fn backpressure_style(len: usize, capacity: usize) -> Style {
    let ratio = if capacity == 0 {
        0.0
    } else {
        len as f64 / capacity as f64
    };
    if ratio >= 0.8 {
        Style::new().red()
    } else if ratio >= 0.5 {
        Style::new().yellow()
    } else {
        Style::new().fg(FG)
    }
}

fn fmt_short(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs < 60.0 {
        format!("{secs:.1}s")
    } else {
        let secs = d.as_secs();
        format!("{}m{:02}s", secs / 60, secs % 60)
    }
}

fn fmt_ago(d: Duration) -> String {
    let secs = d.as_secs();
    if secs < 60 {
        format!("{secs}s ago")
    } else if secs < 3600 {
        format!("{}m {:02}s ago", secs / 60, secs % 60)
    } else {
        format!("{}h {:02}m ago", secs / 3600, (secs % 3600) / 60)
    }
}

fn dash_or<T: ToString>(value: Option<T>) -> String {
    value.map_or_else(|| "-".to_string(), |v| v.to_string())
}

fn right(text: String) -> Line<'static> {
    Line::from(text).right_aligned()
}

/// Border/title color for a tiled panel: cyan when it holds focus, neutral otherwise.
fn panel_accent(focused: bool) -> Style {
    if focused {
        Style::new().cyan()
    } else {
        Style::new().fg(FG)
    }
}

/// Renders `samples` (oldest→newest) as a braille sparkline `width` cells wide. Each cell packs
/// two samples (left = older, right = newer), each scaled to 1–4 dots high against `max` so there
/// is always a baseline. The series is right-aligned and zero-padded on the left, so it scrolls
/// left as samples arrive. Cells with no traffic are dim grey; cells with activity are cyan
/// (btop-style), one styled span per cell since a braille glyph has a single color.
fn sparkline_line(samples: &[u64], max: u64, width: usize) -> Line<'static> {
    let cols = width * 2;
    let mut data = vec![0u64; cols.saturating_sub(samples.len())];
    data.extend_from_slice(&samples[samples.len().saturating_sub(cols)..]);

    let spans: Vec<Span> = data
        .chunks(2)
        .map(|pair| {
            let (left, right) = (pair[0], pair.get(1).copied().unwrap_or(0));
            let ch = braille(spark_height(left, max), spark_height(right, max));
            let color = if left > 0 || right > 0 {
                SPARK_ACTIVE
            } else {
                SPARK_IDLE
            };
            Span::styled(ch.to_string(), Style::new().fg(color))
        })
        .collect();
    Line::from(spans)
}

/// Scales a value to a 1–4 dot column. Every column keeps at least the bottom dot, so the graph
/// shows a continuous baseline even while idle.
fn spark_height(value: u64, max: u64) -> u8 {
    if max == 0 {
        return 1;
    }
    (((value as f64 / max as f64) * 4.0).round() as u8).clamp(1, 4)
}

/// A single braille cell with the left and right columns lit to the given dot heights (0–4),
/// filled from the bottom up. Base code point U+2800.
fn braille(left: u8, right: u8) -> char {
    // Left column bottom→top dots: 7,3,2,1; right column: 8,6,5,4.
    const LEFT: [u8; 5] = [0x00, 0x40, 0x44, 0x46, 0x47];
    const RIGHT: [u8; 5] = [0x00, 0x80, 0xA0, 0xB0, 0xB8];
    let bits = LEFT[left.min(4) as usize] | RIGHT[right.min(4) as usize];
    char::from_u32(0x2800 + bits as u32).unwrap_or(' ')
}

/// Blends a color toward `BG` by `factor` (1.0 = unchanged, 0.0 = fully `BG`).
fn fade_toward_bg(color: Color, factor: f32) -> Color {
    let (tr, tg, tb) = color_rgb(BG);
    let (r, g, b) = color_rgb(color);
    let lerp = |c: u8, target: u8| (target as f32 + (c as f32 - target as f32) * factor) as u8;
    Color::Rgb(lerp(r, tr), lerp(g, tg), lerp(b, tb))
}

/// Approximate RGB for a color, so the fade can blend named ANSI colors as well as `Rgb`.
/// Unset/default fg is treated as the normal foreground.
fn color_rgb(color: Color) -> (u8, u8, u8) {
    match color {
        Color::Rgb(r, g, b) => (r, g, b),
        Color::Red | Color::LightRed => (235, 80, 80),
        Color::Yellow | Color::LightYellow => (220, 180, 90),
        Color::Green | Color::LightGreen => (143, 196, 110),
        Color::Cyan | Color::LightCyan => (110, 180, 200),
        Color::Black => (0, 0, 0),
        Color::DarkGray => (120, 120, 128),
        _ => (205, 205, 212), // White/Gray/Reset/… ≈ FG
    }
}

/// A `width`×`height` rectangle centered within `area` (clamped to fit).
fn centered_rect(area: Rect, width: u16, height: u16) -> Rect {
    let width = width.min(area.width);
    let height = height.min(area.height);
    Rect {
        x: area.x + (area.width - width) / 2,
        y: area.y + (area.height - height) / 2,
        width,
        height,
    }
}

/// The keybinding reference shown in the `?` help popup. Column-sort keys are omitted on
/// purpose — they're discoverable as the red letters in the table header.
fn help_lines() -> Vec<Line<'static>> {
    let key = |keys: &str, desc: &str| {
        Line::from(vec![
            Span::styled(format!("  {keys:<16}"), Style::new().cyan()),
            Span::raw(desc.to_string()),
        ])
    };
    let head = |title: &str| Line::from(format!("  {title}")).bold();
    vec![
        head("Navigation"),
        key("↑/k   ↓/j", "move selection"),
        key("Home / End", "first / last"),
        key("← / h", "collapse node"),
        key("→ / l", "expand node"),
        key("Space", "toggle collapse"),
        key("c", "collapse all"),
        key("e", "expand all"),
        Line::raw(""),
        head("View"),
        key("↵ Enter", "inspect actor (toggle)"),
        key("Tab", "focus inspect panel"),
        key("/", "filter"),
        key("- / +", "poll interval"),
        Line::raw(""),
        head("Inspect panel (focused)"),
        key("j/k   ↑/↓", "scroll"),
        key("PgUp / PgDn", "scroll page"),
        key("Tab", "back to list"),
        Line::raw(""),
        head("Sorting"),
        Line::from("  press the red letter in a column header").dim(),
        Line::raw(""),
        head("General"),
        key("?", "toggle this help"),
        key("q / Esc", "quit"),
    ]
}

fn build_panel_lines(
    actor: &ActorSnapshot,
    row: &ActorRow,
    snapshot: &Snapshot,
    deadlocks: &[Vec<ActorId>],
    history: Option<&VecDeque<u64>>,
) -> Vec<Line<'static>> {
    let name = |id: &ActorId| {
        snapshot
            .actors
            .iter()
            .find(|a| &a.id == id)
            .map(|a| short_type_name(&a.name).to_string())
            .unwrap_or_else(|| "(gone)".to_string())
    };
    // The panel's box title already shows the id + name, so start straight at the fields.
    let mut lines = Vec::new();

    let (state_text, state_style) = row.state_cell();
    lines.push(field_spans(
        "State",
        vec![
            Span::styled("●", state_style),
            Span::raw(format!(" {state_text}")),
        ],
    ));
    let handling = match &actor.handling {
        Some(h) => format!("{} ({})", short_type_name(&h.message), fmt_short(h.elapsed)),
        None => "idle".to_string(),
    };
    lines.push(field("Handling", Span::raw(handling)));

    if let Some(wait) = &actor.waiting_on {
        let kind = wait_kind_word(wait.kind);
        let edge = format!(
            "#{:03} {} · {kind} · {}",
            wait.target.0,
            name(&wait.target),
            fmt_short(wait.elapsed)
        );
        let style = if row.deadlocked {
            Style::new().red()
        } else {
            Style::new()
        };
        lines.push(field("Waiting on", Span::styled(edge, style)));
    }

    if let Some(cycle) = deadlocks.iter().find(|c| c.contains(&actor.id)) {
        let chain = fmt_chain_from(cycle, actor.id);
        lines.push(Line::from(format!("⚠ DEADLOCK  {chain}")).red().bold());
    }

    lines.push(Line::raw(""));

    let age = snapshot
        .captured_at
        .duration_since(actor.spawned_at)
        .unwrap_or_default();
    lines.push(field("Started", Span::raw(fmt_ago(age))));

    let mailbox = match actor.mailbox.capacity {
        Some(cap) => {
            let (text, style) = mailbox_bar(actor.mailbox.len, cap);
            Span::styled(text, style)
        }
        None => Span::raw(format!("{} / ∞ (unbounded)", actor.mailbox.len)),
    };
    lines.push(field("Mailbox", mailbox));

    let throughput = match row.msg_per_sec {
        Some(now) => match history.filter(|h| !h.is_empty()) {
            Some(h) => {
                let avg = h.iter().sum::<u64>() / h.len() as u64;
                let peak = h.iter().copied().max().unwrap_or(0);
                format!("{now} now · {avg} avg · {peak} peak")
            }
            None => format!("{now} msg/s"),
        },
        None => "—".to_string(),
    };
    lines.push(field("Throughput", Span::raw(throughput)));
    lines.push(field(
        "Refs",
        Span::raw(format!(
            "{} strong · {} weak",
            actor.refs.strong, actor.refs.weak
        )),
    ));
    lines.push(Line::raw(""));

    match &actor.links.parent {
        Some(parent) => {
            let strat = snapshot
                .actors
                .iter()
                .find(|a| &a.id == parent)
                .and_then(|p| p.strategy)
                .map(strategy_label)
                .unwrap_or("?");
            lines.push(field(
                "Supervisor",
                Span::raw(format!("#{:03} {} ({strat})", parent.0, name(parent))),
            ));
        }
        None => lines.push(field("Supervisor", Span::raw("none (root)").dim())),
    }
    if let Some(strategy) = actor.strategy {
        lines.push(field("Strategy", Span::raw(strategy_label(strategy))));
    }
    if let Some(sup) = &actor.supervision {
        let policy = restart_policy_word(sup.policy);
        let unlimited = sup.max_restarts == u32::MAX;
        let detail = if unlimited {
            format!("{policy} · {} restarts · no limit", sup.restart_count)
        } else {
            format!(
                "{policy} · {}/{} in {}s",
                sup.restart_count,
                sup.max_restarts,
                sup.restart_window.as_secs()
            )
        };
        let mut spans = vec![
            Span::styled(format!("{:<11} ", "Policy"), Style::new().dim()),
            Span::raw(detail),
        ];
        if !unlimited && sup.restart_count >= sup.max_restarts {
            spans.push(Span::styled("  ⚠ AT LIMIT", Style::new().red().bold()));
        }
        lines.push(Line::from(spans));
    }
    lines.push(field(
        "Restarts",
        Span::raw(format!(
            "{} · {} panics",
            actor.counters.restarts, actor.counters.panics
        )),
    ));

    if !actor.links.children.is_empty() {
        lines.push(Line::raw(""));
        lines.push(Line::from("Children").dim());
        for child in &actor.links.children {
            lines.push(link_line("  ", *child, snapshot));
        }
    }

    if !actor.links.siblings.is_empty() {
        lines.push(Line::raw(""));
        lines.push(Line::from("Sibling links").dim());
        for sibling in &actor.links.siblings {
            lines.push(link_line("  ↔ ", *sibling, snapshot));
        }
    }

    let waiters: Vec<&ActorSnapshot> = snapshot
        .actors
        .iter()
        .filter(|a| a.waiting_on.as_ref().is_some_and(|w| w.target == actor.id))
        .collect();
    if !waiters.is_empty() {
        lines.push(Line::raw(""));
        lines.push(Line::from("Waiters (blocked on this)").dim());
        for waiter in waiters {
            let wait = waiter.waiting_on.as_ref().unwrap();
            lines.push(Line::from(format!(
                "  #{:03} {} · {} · {}",
                waiter.id.0,
                short_type_name(&waiter.name),
                wait_kind_word(wait.kind),
                fmt_short(wait.elapsed)
            )));
        }
    }

    if !actor.message_types.is_empty() {
        let total: u64 = actor.message_types.iter().map(|m| m.count).sum();
        lines.push(Line::raw(""));
        lines.push(Line::from("Messages by type").dim());
        for mc in actor.message_types.iter().take(6) {
            let frac = mc.count as f64 / total.max(1) as f64;
            let filled = (frac * 10.0).round() as usize;
            let bar = format!("{}{}", "█".repeat(filled), "░".repeat(10 - filled));
            // Truncate by chars, not bytes, so a multibyte type name can't panic on a non-boundary.
            let name: String = short_type_name(&mc.name).chars().take(16).collect();
            lines.push(Line::from(format!(
                "  {name:<16} {:>3}% {bar}",
                (frac * 100.0).round() as u64
            )));
        }
    }

    lines
}

/// Strips the module path (and generic args) from a type name: `a::b::Foo<X>` → `Foo`.
fn short_type_name(name: &str) -> &str {
    let head = name.split('<').next().unwrap_or(name);
    head.rsplit("::").next().unwrap_or(head)
}

fn wait_kind_word(kind: WaitKind) -> &'static str {
    match kind {
        WaitKind::Ask => "ask",
        WaitKind::Tell => "tell",
    }
}

fn restart_policy_word(policy: RestartPolicy) -> &'static str {
    match policy {
        RestartPolicy::Permanent => "Permanent",
        RestartPolicy::Transient => "Transient",
        RestartPolicy::Never => "Never",
    }
}

/// Formats a cycle as a chain of ids starting from `start` and looping back to it.
fn fmt_chain_from(cycle: &[ActorId], start: ActorId) -> String {
    let offset = cycle.iter().position(|&id| id == start).unwrap_or(0);
    let mut ids: Vec<ActorId> = cycle[offset..].to_vec();
    ids.extend_from_slice(&cycle[..offset]);
    ids.push(start);
    ids.iter()
        .map(|id| format!("#{:03}", id.0))
        .collect::<Vec<_>>()
        .join(" → ")
}

fn fmt_cycle(cycle: &[ActorId], names: &HashMap<ActorId, &str>) -> String {
    let label = |id: &ActorId| format!("#{:03} {}", id.0, names.get(id).copied().unwrap_or("?"));
    if cycle.len() == 2 {
        format!("{} ⇄ {}", label(&cycle[0]), label(&cycle[1]))
    } else {
        let parts: Vec<String> = cycle.iter().map(&label).collect();
        format!("{} → {}", parts.join(" → "), label(&cycle[0]))
    }
}

fn field<'a>(label: &str, value: Span<'a>) -> Line<'a> {
    field_spans(label, vec![value])
}

fn field_spans<'a>(label: &str, values: Vec<Span<'a>>) -> Line<'a> {
    let mut spans = vec![Span::styled(format!("{label:<11} "), Style::new().dim())];
    spans.extend(values);
    Line::from(spans)
}

fn link_line(prefix: &str, id: ActorId, snapshot: &Snapshot) -> Line<'static> {
    match snapshot.actors.iter().find(|a| a.id == id) {
        Some(a) => Line::from(format!(
            "{prefix}#{:03} {} · {}",
            id.0,
            short_type_name(&a.name),
            status_word(&a.status)
        )),
        None => Line::from(format!("{prefix}#{:03} (gone)", id.0)).dim(),
    }
}

fn mailbox_bar(len: usize, capacity: usize) -> (String, Style) {
    let ratio = if capacity == 0 {
        0.0
    } else {
        (len as f64 / capacity as f64).clamp(0.0, 1.0)
    };
    let filled = (ratio * 10.0).round() as usize;
    let bar = format!("{}{}", "█".repeat(filled), "░".repeat(10 - filled));
    let pct = (ratio * 100.0).round() as u64;
    (
        format!("{len} / {capacity}  {bar}  {pct}%"),
        backpressure_style(len, capacity),
    )
}

fn fmt_uptime(d: Duration) -> String {
    let secs = d.as_secs();
    format!(
        "{:02}:{:02}:{:02}",
        secs / 3600,
        (secs % 3600) / 60,
        secs % 60
    )
}
