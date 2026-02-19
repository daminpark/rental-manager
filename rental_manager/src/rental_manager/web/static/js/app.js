// Rental Manager Dashboard Application
// Note: This is an internal admin dashboard. Data is sourced from trusted API endpoints
// and user-controlled configuration. In a production deployment with untrusted data,
// consider adding DOMPurify or similar sanitization.

// Resolve API base relative to current path (supports HA ingress)
const API_BASE = new URL('api', window.location.href).pathname.replace(/\/$/, '');

// State
let state = {
    locks: [],
    bookings: [],
    calendars: [],
    emergencyCodes: [],
    houseCode: '',
    currentView: 'locks',
    selectedLock: null,
    lastSyncStatus: null,
};

// Utility: escape HTML for safe rendering
function escapeHtml(text) {
    if (text === null || text === undefined) return '';
    const div = document.createElement('div');
    div.textContent = String(text);
    return div.innerHTML;
}

// API Functions
async function api(endpoint, options = {}) {
    const response = await fetch(`${API_BASE}${endpoint}`, {
        headers: {
            'Content-Type': 'application/json',
            ...options.headers,
        },
        ...options,
    });

    if (!response.ok) {
        const error = await response.json().catch(() => ({ detail: 'Unknown error' }));
        throw new Error(error.detail || 'API request failed');
    }

    return response.json();
}

// Load house info from /api/info
async function loadHouseInfo() {
    try {
        const info = await api('/info');
        state.houseCode = info.house_code;
        const subtitle = document.getElementById('house-subtitle');
        if (subtitle) {
            subtitle.textContent = `${info.house_code} Vauxhall Bridge Road`;
        }
        document.title = `Rental Manager - ${info.house_code} VBR`;
    } catch (error) {
        console.error('Failed to load house info:', error);
    }
}

// Data Loading
async function loadLocks() {
    state.locks = await api('/locks');
    renderLocks();
}

async function loadBookings() {
    state.bookings = await api('/bookings');
    renderBookings();
}

async function loadCalendars() {
    state.calendars = await api('/calendars');
    renderCalendars();
}

async function loadSyncStatus() {
    const status = await api('/sync-status');
    renderSyncStatus(status);
}

// Rendering Functions
function renderLocks() {
    const container = document.getElementById('lock-grid');
    if (!container) return;

    // Clear existing content
    container.textContent = '';

    state.locks.forEach(lock => {
        const card = document.createElement('div');
        card.className = 'lock-card';
        card.dataset.lockId = lock.entity_id;

        const header = document.createElement('div');
        header.className = 'lock-card-header';

        const nameDiv = document.createElement('div');
        const lockName = document.createElement('div');
        lockName.className = 'lock-name';
        lockName.textContent = lock.name;
        nameDiv.appendChild(lockName);

        const activeCodes = countActiveCodes(lock);
        const statusDiv = document.createElement('div');
        statusDiv.className = `lock-status ${activeCodes > 0 ? 'locked' : 'unknown'}`;
        const statusDot = document.createElement('span');
        statusDot.className = 'lock-status-dot';
        statusDiv.appendChild(statusDot);
        statusDiv.appendChild(document.createTextNode(` ${activeCodes} active`));

        header.appendChild(nameDiv);
        header.appendChild(statusDiv);

        const info = document.createElement('div');
        info.className = 'lock-info';

        const autoLockLabel = lock.auto_lock_enabled === true ? 'On' : lock.auto_lock_enabled === false ? 'Off' : '---';

        const infoItems = [
            { label: 'Type', value: lock.lock_type },
            { label: 'Master Code', value: lock.master_code || '---' },
            { label: 'Auto-Lock', value: autoLockLabel },
            { label: 'Emergency', value: lock.emergency_code || '---' },
        ];

        infoItems.forEach(item => {
            const infoItem = document.createElement('div');
            infoItem.className = 'lock-info-item';
            const label = document.createElement('div');
            label.className = 'lock-info-label';
            label.textContent = item.label;
            const value = document.createElement('div');
            value.className = 'lock-info-value';
            value.textContent = item.value;
            infoItem.appendChild(label);
            infoItem.appendChild(value);
            info.appendChild(infoItem);
        });

        const actions = document.createElement('div');
        actions.className = 'lock-actions';

        const detailsBtn = document.createElement('button');
        detailsBtn.className = 'btn btn-sm btn-secondary';
        detailsBtn.textContent = 'Details';
        detailsBtn.addEventListener('click', () => showLockDetail(lock.entity_id));

        const unlockBtn = document.createElement('button');
        unlockBtn.className = 'btn btn-sm btn-primary';
        unlockBtn.textContent = 'Unlock';
        unlockBtn.addEventListener('click', () => lockAction(lock.entity_id, 'unlock'));

        const lockBtn = document.createElement('button');
        lockBtn.className = 'btn btn-sm btn-success';
        lockBtn.textContent = 'Lock';
        lockBtn.addEventListener('click', () => lockAction(lock.entity_id, 'lock'));

        actions.appendChild(detailsBtn);
        actions.appendChild(unlockBtn);
        actions.appendChild(lockBtn);

        card.appendChild(header);
        card.appendChild(info);
        card.appendChild(actions);
        container.appendChild(card);
    });
}

function countActiveCodes(lock) {
    // Count guest slots (2-18) that have an active assignment
    return lock.slots.filter(s => s.is_active && s.slot_number > 1 && s.slot_number < 20).length;
}

function renderBookings() {
    const container = document.getElementById('bookings-table');
    if (!container) return;

    const today = new Date().toISOString().slice(0, 10);
    const activeBookings = state.bookings.filter(b => !b.is_blocked);

    // Group by calendar_id
    const grouped = {};
    activeBookings.forEach(b => {
        if (!grouped[b.calendar_id]) grouped[b.calendar_id] = [];
        grouped[b.calendar_id].push(b);
    });

    // Sort each group: upcoming/current first (by check_in_date), past at end
    for (const calId of Object.keys(grouped)) {
        grouped[calId].sort((a, b) => a.check_in_date.localeCompare(b.check_in_date));
        // Move past bookings (check_out_date < today) to the end
        const upcoming = grouped[calId].filter(b => b.check_out_date >= today);
        const past = grouped[calId].filter(b => b.check_out_date < today);
        grouped[calId] = [...upcoming, ...past];
    }

    // Sort calendar groups
    const sortedCalIds = Object.keys(grouped).sort((a, b) => a.localeCompare(b));

    container.textContent = '';

    const PREVIEW_COUNT = 3;

    sortedCalIds.forEach(calId => {
        const bookings = grouped[calId];
        const calName = getCalendarDisplayName(calId);

        // Listing card
        const card = document.createElement('div');
        card.className = 'booking-listing-card';
        card.style.marginBottom = '1rem';

        // Header — clickable to expand
        const header = document.createElement('div');
        header.className = 'booking-listing-header';
        header.style.cursor = 'pointer';
        header.style.display = 'flex';
        header.style.justifyContent = 'space-between';
        header.style.alignItems = 'center';
        header.style.padding = '0.75rem 1rem';
        header.style.background = 'var(--bg-tertiary)';
        header.style.borderRadius = '8px 8px 0 0';
        header.style.borderBottom = '1px solid var(--border-color)';

        const titleDiv = document.createElement('div');
        const title = document.createElement('span');
        title.style.fontWeight = '600';
        title.style.fontSize = '1rem';
        title.textContent = calName;
        titleDiv.appendChild(title);

        const countBadge = document.createElement('span');
        countBadge.className = 'badge badge-info';
        countBadge.style.marginLeft = '0.5rem';
        const upcomingCount = bookings.filter(b => b.check_out_date >= today).length;
        countBadge.textContent = `${upcomingCount} upcoming`;
        titleDiv.appendChild(countBadge);

        const expandIcon = document.createElement('span');
        expandIcon.className = 'expand-icon';
        expandIcon.textContent = bookings.length > PREVIEW_COUNT ? `+${bookings.length - PREVIEW_COUNT} more` : '';
        expandIcon.style.color = 'var(--text-secondary)';
        expandIcon.style.fontSize = '0.85rem';

        header.appendChild(titleDiv);
        header.appendChild(expandIcon);

        // Booking rows container
        const bodyDiv = document.createElement('div');
        bodyDiv.style.background = 'var(--bg-secondary)';
        bodyDiv.style.borderRadius = '0 0 8px 8px';
        bodyDiv.style.overflow = 'hidden';

        const table = document.createElement('table');
        table.style.width = '100%';
        table.style.marginBottom = '0';

        const thead = document.createElement('thead');
        const headerRow = document.createElement('tr');
        ['Guest', 'Check-in', 'Check-out', 'Code', 'Channel', ''].forEach(text => {
            const th = document.createElement('th');
            th.textContent = text;
            th.style.padding = '0.5rem 0.75rem';
            th.style.fontSize = '0.8rem';
            headerRow.appendChild(th);
        });
        thead.appendChild(headerRow);
        table.appendChild(thead);

        const tbody = document.createElement('tbody');
        let expanded = false;

        bookings.forEach((booking, idx) => {
            const row = createBookingRow(booking, today);
            if (idx >= PREVIEW_COUNT) {
                row.classList.add('booking-extra-row');
                row.style.display = 'none';
            }
            tbody.appendChild(row);
        });

        table.appendChild(tbody);
        bodyDiv.appendChild(table);

        // Click header to expand/collapse
        header.addEventListener('click', () => {
            expanded = !expanded;
            const extraRows = tbody.querySelectorAll('.booking-extra-row');
            extraRows.forEach(r => {
                r.style.display = expanded ? '' : 'none';
            });
            if (bookings.length > PREVIEW_COUNT) {
                expandIcon.textContent = expanded
                    ? 'Show less'
                    : `+${bookings.length - PREVIEW_COUNT} more`;
            }
        });

        card.appendChild(header);
        card.appendChild(bodyDiv);
        container.appendChild(card);
    });
}

function createBookingRow(booking, today) {
    const row = document.createElement('tr');

    const isPast = booking.check_out_date < today;
    const isCurrent = booking.check_in_date <= today && booking.check_out_date >= today;
    if (isPast) row.style.opacity = '0.5';
    if (isCurrent) row.style.borderLeft = '3px solid var(--accent-green)';
    if (booking.code_disabled) {
        row.style.opacity = '0.5';
        row.style.textDecoration = 'line-through';
    }

    const guestCell = document.createElement('td');
    guestCell.textContent = booking.guest_name;
    guestCell.style.padding = '0.5rem 0.75rem';
    row.appendChild(guestCell);

    const checkinCell = document.createElement('td');
    checkinCell.textContent = booking.check_in_date;
    checkinCell.style.padding = '0.5rem 0.75rem';
    row.appendChild(checkinCell);

    const checkoutCell = document.createElement('td');
    checkoutCell.textContent = booking.check_out_date;
    checkoutCell.style.padding = '0.5rem 0.75rem';
    row.appendChild(checkoutCell);

    const codeCell = document.createElement('td');
    codeCell.style.padding = '0.5rem 0.75rem';
    if (booking.code_disabled) {
        const disabledBadge = document.createElement('span');
        disabledBadge.className = 'badge badge-danger';
        disabledBadge.textContent = 'DISABLED';
        disabledBadge.style.fontSize = '0.7rem';
        codeCell.appendChild(disabledBadge);
    } else {
        const codeEl = document.createElement('code');
        codeEl.textContent = booking.code || '---';
        codeCell.appendChild(codeEl);
        if (booking.code_locked) {
            const lockIcon = document.createElement('span');
            lockIcon.textContent = ' \u{1F512}';
            lockIcon.title = 'Code finalized';
            lockIcon.style.fontSize = '0.75rem';
            codeCell.appendChild(lockIcon);
        }
    }
    row.appendChild(codeCell);

    const channelCell = document.createElement('td');
    channelCell.style.padding = '0.5rem 0.75rem';
    const badge = document.createElement('span');
    badge.className = 'badge badge-info';
    badge.textContent = booking.channel || 'N/A';
    channelCell.appendChild(badge);
    row.appendChild(channelCell);

    const actionsCell = document.createElement('td');
    actionsCell.style.padding = '0.5rem 0.75rem';
    actionsCell.style.whiteSpace = 'nowrap';

    const timesBtn = document.createElement('button');
    timesBtn.className = 'btn btn-sm btn-secondary';
    timesBtn.textContent = 'Set Times';
    timesBtn.addEventListener('click', (e) => {
        e.stopPropagation();
        showTimeOverride(booking.id);
    });
    actionsCell.appendChild(timesBtn);

    if (!isPast) {
        const codeBtn = document.createElement('button');
        codeBtn.className = 'btn btn-sm btn-secondary';
        codeBtn.textContent = 'Set Code';
        codeBtn.style.marginLeft = '0.25rem';
        codeBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            setBookingCode(booking.id);
        });
        actionsCell.appendChild(codeBtn);

        if (!booking.code_disabled && booking.code) {
            const recodeBtn = document.createElement('button');
            recodeBtn.className = 'btn btn-sm btn-primary';
            recodeBtn.textContent = 'Recode';
            recodeBtn.style.marginLeft = '0.25rem';
            recodeBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                recodeBooking(booking.id, booking.guest_name);
            });
            actionsCell.appendChild(recodeBtn);
        }

        const toggleBtn = document.createElement('button');
        toggleBtn.className = booking.code_disabled
            ? 'btn btn-sm btn-success'
            : 'btn btn-sm btn-danger';
        toggleBtn.textContent = booking.code_disabled ? 'Enable' : 'Disable';
        toggleBtn.style.marginLeft = '0.25rem';
        toggleBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            toggleBookingCode(booking.id, booking.code_disabled);
        });
        actionsCell.appendChild(toggleBtn);
    }

    row.appendChild(actionsCell);

    return row;
}

async function toggleBookingCode(bookingId, currentlyDisabled) {
    const action = currentlyDisabled ? 'enable' : 'disable';
    const confirmMsg = currentlyDisabled
        ? 'Re-enable this guest\'s code on all locks?'
        : 'Disable this guest\'s code on all locks? The code will be cleared immediately.';

    if (!confirm(confirmMsg)) return;

    try {
        const result = await api(`/bookings/${bookingId}/${action}-code`, {
            method: 'POST',
        });
        let msg = `Code ${action}d successfully`;
        if (result.locks_cleared) msg += ` (${result.locks_cleared} lock(s) cleared)`;
        if (result.locks_activated) msg += ` (${result.locks_activated} lock(s) activated)`;
        if (result.locks_rescheduled) msg += ` (${result.locks_rescheduled} rescheduled)`;
        showToast(msg, 'success');
        loadBookings();
    } catch (error) {
        showToast(error.message, 'error');
    }
}

async function recodeBooking(bookingId, guestName) {
    if (!confirm(`Re-send codes to all locks for ${guestName}? This will only work for locks currently within the active time window.`)) return;

    try {
        const result = await api(`/bookings/${bookingId}/recode`, {
            method: 'POST',
        });

        if (result.status === 'disabled') {
            showToast(result.message, 'warning');
        } else if (result.status === 'no_code') {
            showToast(result.message, 'warning');
        } else if (result.status === 'outside_window') {
            showToast(result.message, 'warning');
        } else {
            let msg = `Recoded ${result.locks_recoded} lock(s) for ${guestName}`;
            if (result.locks_skipped) msg += ` (${result.locks_skipped} outside window)`;
            if (result.errors && result.errors.length) msg += ` — ${result.errors.length} failed`;
            showToast(msg, result.errors && result.errors.length ? 'warning' : 'success');
        }

        loadBookings();
    } catch (error) {
        showToast(error.message, 'error');
    }
}

function getCalendarDisplayName(calendarId) {
    const names = {
        '195_room_1': '195 Room 1',
        '195_room_2': '195 Room 2',
        '195_room_3': '195 Room 3',
        '195_room_4': '195 Room 4',
        '195_room_5': '195 Room 5',
        '195_room_6': '195 Room 6',
        '195_suite_a': '195 Suite A',
        '195_suite_b': '195 Suite B',
        '195vbr': '195 Whole House',
        '193_room_1': '193 Room 1',
        '193_room_2': '193 Room 2',
        '193_room_3': '193 Room 3',
        '193_room_4': '193 Room 4',
        '193_room_5': '193 Room 5',
        '193_room_6': '193 Room 6',
        '193_suite_a': '193 Suite A',
        '193_suite_b': '193 Suite B',
        '193vbr': '193 Whole House',
        '193195vbr': '193 & 195 Both Houses',
    };
    return names[calendarId] || calendarId;
}

function renderCalendars() {
    const container = document.getElementById('calendars-list');
    if (!container) return;

    container.textContent = '';

    state.calendars.forEach(cal => {
        const card = document.createElement('div');
        card.className = 'card';
        card.style.marginBottom = '0.75rem';

        const header = document.createElement('div');
        header.className = 'card-header';

        const title = document.createElement('span');
        title.className = 'card-title';
        title.textContent = cal.name;

        const badgeWrap = document.createElement('div');
        badgeWrap.style.display = 'flex';
        badgeWrap.style.gap = '0.5rem';

        if (cal.ha_entity_id) {
            const haBadge = document.createElement('span');
            haBadge.className = 'badge badge-success';
            haBadge.textContent = 'HA Entity';
            haBadge.style.fontSize = '0.7rem';
            badgeWrap.appendChild(haBadge);
        }

        const badge = document.createElement('span');
        badge.className = cal.last_fetch_error ? 'badge badge-danger' : 'badge badge-success';
        badge.textContent = cal.last_fetch_error ? 'Error' : 'OK';
        badgeWrap.appendChild(badge);

        header.appendChild(title);
        header.appendChild(badgeWrap);

        const info = document.createElement('div');
        info.style.fontSize = '0.875rem';
        info.style.color = 'var(--text-secondary)';

        const idP = document.createElement('p');
        const idStrong = document.createElement('strong');
        idStrong.textContent = 'ID: ';
        idP.appendChild(idStrong);
        idP.appendChild(document.createTextNode(cal.calendar_id));
        info.appendChild(idP);

        const typeP = document.createElement('p');
        const typeStrong = document.createElement('strong');
        typeStrong.textContent = 'Type: ';
        typeP.appendChild(typeStrong);
        typeP.appendChild(document.createTextNode(cal.calendar_type));
        info.appendChild(typeP);

        if (cal.ha_entity_id) {
            const entityP = document.createElement('p');
            const entityStrong = document.createElement('strong');
            entityStrong.textContent = 'HA Entity: ';
            entityP.appendChild(entityStrong);
            entityP.appendChild(document.createTextNode(cal.ha_entity_id));
            info.appendChild(entityP);
        }

        const fetchedP = document.createElement('p');
        const fetchedStrong = document.createElement('strong');
        fetchedStrong.textContent = 'Last fetched: ';
        fetchedP.appendChild(fetchedStrong);
        fetchedP.appendChild(document.createTextNode(cal.last_fetched || 'Never'));
        info.appendChild(fetchedP);

        if (cal.last_fetch_error) {
            const errorP = document.createElement('p');
            errorP.style.color = 'var(--accent-red)';
            const errorStrong = document.createElement('strong');
            errorStrong.textContent = 'Error: ';
            errorP.appendChild(errorStrong);
            errorP.appendChild(document.createTextNode(cal.last_fetch_error));
            info.appendChild(errorP);
        }

        const inputDiv = document.createElement('div');
        inputDiv.style.marginTop = '0.75rem';

        // HA Entity ID input
        const entityLabel = document.createElement('label');
        entityLabel.className = 'form-label';
        entityLabel.textContent = 'HA Calendar Entity';
        entityLabel.style.fontSize = '0.8rem';
        inputDiv.appendChild(entityLabel);

        const entityInput = document.createElement('input');
        entityInput.type = 'text';
        entityInput.className = 'form-input';
        entityInput.value = cal.ha_entity_id || '';
        entityInput.placeholder = 'calendar.195_1_calendar';
        entityInput.id = `entity-${cal.calendar_id}`;
        entityInput.style.marginBottom = '0.5rem';
        inputDiv.appendChild(entityInput);

        const entityBtn = document.createElement('button');
        entityBtn.className = 'btn btn-sm btn-primary';
        entityBtn.textContent = 'Save Entity';
        entityBtn.style.marginBottom = '0.75rem';
        entityBtn.addEventListener('click', () => updateCalendarEntity(cal.calendar_id));
        inputDiv.appendChild(entityBtn);

        // iCal URL input (fallback)
        const urlLabel = document.createElement('label');
        urlLabel.className = 'form-label';
        urlLabel.textContent = 'iCal URL (fallback)';
        urlLabel.style.fontSize = '0.8rem';
        inputDiv.appendChild(urlLabel);

        const input = document.createElement('input');
        input.type = 'text';
        input.className = 'form-input';
        input.value = cal.ical_url || '';
        input.placeholder = 'iCal URL';
        input.id = `url-${cal.calendar_id}`;
        inputDiv.appendChild(input);

        const updateBtn = document.createElement('button');
        updateBtn.className = 'btn btn-sm btn-primary';
        updateBtn.style.marginTop = '0.5rem';
        updateBtn.textContent = 'Update URL';
        updateBtn.addEventListener('click', () => updateCalendarUrl(cal.calendar_id));
        inputDiv.appendChild(updateBtn);

        card.appendChild(header);
        card.appendChild(info);
        card.appendChild(inputDiv);
        container.appendChild(card);
    });
}

function renderSyncStatus(status) {
    const container = document.getElementById('sync-status');
    if (!container) return;

    container.textContent = '';

    const statusDiv = document.createElement('div');
    statusDiv.className = 'sync-status';

    const icon = document.createElement('span');
    icon.className = 'sync-status-icon';
    if (status.failed_count > 0) {
        icon.classList.add('failed');
    } else if (status.syncing_count > 0) {
        icon.classList.add('syncing');
    } else {
        icon.classList.add('active');
    }

    const text = document.createElement('span');
    if (status.failed_count > 0) {
        text.textContent = `${status.failed_count} failed`;
    } else if (status.syncing_count > 0) {
        text.textContent = `${status.syncing_count} syncing`;
    } else {
        text.textContent = 'All synced';
    }

    statusDiv.appendChild(icon);
    statusDiv.appendChild(text);
    container.appendChild(statusDiv);

    // Cache sync status for the activity view
    state.lastSyncStatus = status;
}

// Activity View (full page)
async function loadActivityView() {
    const content = document.getElementById('activity-log-content');
    const failedSection = document.getElementById('activity-failed-section');
    if (!content) return;

    content.innerHTML = '<div class="loading"><div class="spinner"></div>Loading activity...</div>';
    failedSection.textContent = '';

    // Render failed items from cached sync status
    const status = state.lastSyncStatus;
    if (status) {
        const hasFailedSlots = status.failed_slots && status.failed_slots.length > 0;
        const hasFailedOps = status.failed_ops && status.failed_ops.length > 0;

        if (hasFailedSlots || hasFailedOps) {
            const failedCard = document.createElement('div');
            failedCard.className = 'card';
            failedCard.style.marginBottom = '1rem';

            const failedHeader = document.createElement('div');
            failedHeader.className = 'card-header';
            failedHeader.innerHTML = '<span class="card-title" style="color:var(--accent-red)">Failed Operations</span>';
            failedCard.appendChild(failedHeader);

            const failedBody = document.createElement('div');
            failedBody.style.padding = '0.75rem';

            const retryAllBtn = document.createElement('button');
            retryAllBtn.className = 'btn btn-sm btn-primary';
            retryAllBtn.style.marginBottom = '0.5rem';
            retryAllBtn.textContent = 'Retry All Failed';
            retryAllBtn.addEventListener('click', retryAllFailed);
            failedBody.appendChild(retryAllBtn);

            if (hasFailedSlots) {
                status.failed_slots.forEach(slot => {
                    failedBody.appendChild(renderFailedSlotRow(slot));
                });
            }
            if (hasFailedOps) {
                status.failed_ops.forEach(op => {
                    failedBody.appendChild(renderFailedOpRow(op));
                });
            }

            failedCard.appendChild(failedBody);
            failedSection.appendChild(failedCard);
        }
    }

    // Load audit log (last 24 hours only)
    try {
        const allLogs = await api('/audit-log?limit=500');
        const cutoff = Date.now() - 24 * 60 * 60 * 1000;
        const logs = allLogs.filter(l =>
            l.action !== 'lock_lock' && l.action !== 'lock_unlock'
            && new Date(l.timestamp).getTime() >= cutoff
        );
        content.textContent = '';

        if (logs.length === 0) {
            content.innerHTML = '<div style="padding:2rem;text-align:center;color:var(--text-secondary)">No activity in the last 24 hours</div>';
            return;
        }

        const groups = groupLogs(logs);

        const table = document.createElement('table');
        table.className = 'activity-table';
        table.style.cssText = 'width:100%;border-collapse:collapse;font-size:0.82rem';

        const thStyle = 'text-align:left;padding:0.5rem 0.75rem;border-bottom:2px solid var(--border-color,#e0e0e0);color:var(--text-secondary);font-weight:600';
        const thead = document.createElement('thead');
        thead.innerHTML = `<tr>
            <th style="${thStyle};white-space:nowrap">Time</th>
            <th style="${thStyle}">Action</th>
            <th style="${thStyle}">Lock</th>
            <th style="${thStyle}">Details</th>
            <th style="${thStyle};text-align:center">Status</th>
        </tr>`;
        table.appendChild(thead);

        const tbody = document.createElement('tbody');
        groups.forEach(group => {
            if (group.type === 'single') {
                tbody.appendChild(renderActivityRow(group.log));
            } else {
                renderGroupedRows(tbody, group);
            }
        });

        table.appendChild(tbody);
        content.appendChild(table);

    } catch (err) {
        content.innerHTML = '<div style="padding:1rem;color:var(--accent-red,#dc3545)">Failed to load activity log</div>';
    }
}

function renderActivityRow(log) {
    const tr = document.createElement('tr');
    tr.style.cssText = 'border-bottom:1px solid var(--border-color,rgba(0,0,0,0.06));transition:background 0.3s';
    if (!log.success) {
        tr.style.background = 'var(--danger-bg,rgba(220,53,69,0.05))';
    }

    const d = new Date(log.timestamp);

    // Time
    const tdTime = document.createElement('td');
    tdTime.style.cssText = 'padding:0.4rem 0.75rem;white-space:nowrap;color:var(--text-secondary)';
    tdTime.textContent = formatActivityTime(d);
    tdTime.title = d.toLocaleString();

    // Action
    const tdAction = document.createElement('td');
    tdAction.style.cssText = 'padding:0.4rem 0.75rem;font-weight:500';
    tdAction.textContent = getActionLabel(log.action);

    // Lock
    const tdLock = document.createElement('td');
    tdLock.style.cssText = 'padding:0.4rem 0.75rem';
    if (log.lock_name) {
        tdLock.textContent = log.lock_name.replace(/ Lock$/, '');
        if (log.slot_number) {
            const slotSpan = document.createElement('span');
            slotSpan.style.cssText = 'color:var(--text-secondary);font-size:0.75rem;margin-left:0.25rem';
            slotSpan.textContent = `s${log.slot_number}`;
            tdLock.appendChild(slotSpan);
        }
    } else if (log.slot_number) {
        tdLock.textContent = `Slot ${log.slot_number}`;
    } else {
        tdLock.style.color = 'var(--text-secondary)';
        tdLock.textContent = '—';
    }

    // Details — prefer details field, fall back to booking info, then error message
    const tdDetails = document.createElement('td');
    tdDetails.style.cssText = 'padding:0.4rem 0.75rem;color:var(--text-secondary);max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap';
    const detailsText = formatLogDetails(log);
    tdDetails.textContent = detailsText;
    if (detailsText) tdDetails.title = detailsText;

    // Status
    const tdStatus = document.createElement('td');
    tdStatus.style.cssText = 'padding:0.4rem 0.75rem;text-align:center;white-space:nowrap';

    if (log.success) {
        const dot = document.createElement('span');
        dot.style.cssText = 'display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--accent-green,#28a745)';
        tdStatus.appendChild(dot);
    } else {
        // Failed — show retry button
        const retryBtn = document.createElement('button');
        retryBtn.className = 'btn btn-sm';
        retryBtn.style.cssText = 'font-size:0.7rem;padding:0.15rem 0.5rem;background:var(--accent-red,#dc3545);color:#fff;border:none;border-radius:4px;cursor:pointer';
        retryBtn.textContent = 'Retry';
        retryBtn.addEventListener('click', async () => {
            retryBtn.disabled = true;
            retryBtn.textContent = '...';
            const ok = await retryAuditLogEntry(log);
            if (ok) {
                // Turn row green
                tr.style.background = 'rgba(40,167,69,0.08)';
                tdStatus.textContent = '';
                const dot = document.createElement('span');
                dot.style.cssText = 'display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--accent-green,#28a745)';
                tdStatus.appendChild(dot);
                loadSyncStatus();
            } else {
                retryBtn.disabled = false;
                retryBtn.textContent = 'Retry';
            }
        });
        tdStatus.appendChild(retryBtn);
    }

    tr.appendChild(tdTime);
    tr.appendChild(tdAction);
    tr.appendChild(tdLock);
    tr.appendChild(tdDetails);
    tr.appendChild(tdStatus);
    return tr;
}

function renderGroupedRows(tbody, group) {
    const logs = group.logs;
    const d = new Date(group.timestamp);
    const allOk = logs.every(l => l.success);
    const failCount = logs.filter(l => !l.success).length;
    const childRows = [];

    // Summary row
    const tr = document.createElement('tr');
    tr.style.cssText = 'border-bottom:1px solid var(--border-color,rgba(0,0,0,0.06));cursor:pointer;transition:background 0.2s';
    if (failCount > 0) tr.style.background = 'var(--danger-bg,rgba(220,53,69,0.05))';

    // Time
    const tdTime = document.createElement('td');
    tdTime.style.cssText = 'padding:0.4rem 0.75rem;white-space:nowrap;color:var(--text-secondary)';
    tdTime.textContent = formatActivityTime(d);
    tdTime.title = d.toLocaleString();

    // Action — show batch label with expand arrow
    const tdAction = document.createElement('td');
    tdAction.style.cssText = 'padding:0.4rem 0.75rem;font-weight:500';
    const arrow = document.createElement('span');
    arrow.textContent = '▸ ';
    arrow.style.cssText = 'font-size:0.7rem;color:var(--text-secondary);transition:transform 0.2s;display:inline-block';
    tdAction.appendChild(arrow);
    tdAction.appendChild(document.createTextNode(getBatchLabel(logs)));

    // Lock — show count
    const tdLock = document.createElement('td');
    tdLock.style.cssText = 'padding:0.4rem 0.75rem;color:var(--text-secondary)';
    const uniqueLocks = [...new Set(logs.map(l => l.lock_name).filter(Boolean))];
    tdLock.textContent = uniqueLocks.length > 3
        ? `${uniqueLocks.length} locks`
        : uniqueLocks.map(n => n.replace(/ Lock$/, '')).join(', ');

    // Details
    const tdDetails = document.createElement('td');
    tdDetails.style.cssText = 'padding:0.4rem 0.75rem;color:var(--text-secondary);max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap';
    tdDetails.textContent = formatLogDetails(logs[0]);

    // Status
    const tdStatus = document.createElement('td');
    tdStatus.style.cssText = 'padding:0.4rem 0.75rem;text-align:center;white-space:nowrap';
    if (allOk) {
        const dot = document.createElement('span');
        dot.style.cssText = 'display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--accent-green,#28a745)';
        tdStatus.appendChild(dot);
    } else {
        const badge = document.createElement('span');
        badge.style.cssText = 'font-size:0.7rem;padding:0.1rem 0.4rem;background:var(--accent-red,#dc3545);color:#fff;border-radius:4px';
        badge.textContent = `${failCount} failed`;
        tdStatus.appendChild(badge);
    }

    tr.appendChild(tdTime);
    tr.appendChild(tdAction);
    tr.appendChild(tdLock);
    tr.appendChild(tdDetails);
    tr.appendChild(tdStatus);
    tbody.appendChild(tr);

    // Child rows (hidden by default)
    logs.forEach(log => {
        const childTr = renderActivityRow(log);
        childTr.style.display = 'none';
        childTr.classList.add('group-child');
        childTr.style.background = 'var(--card-bg,rgba(0,0,0,0.015))';
        // Indent the action cell
        const actionCell = childTr.children[1];
        if (actionCell) actionCell.style.paddingLeft = '1.5rem';
        childRows.push(childTr);
        tbody.appendChild(childTr);
    });

    // Toggle expand/collapse
    let expanded = false;
    tr.addEventListener('click', () => {
        expanded = !expanded;
        arrow.style.transform = expanded ? 'rotate(90deg)' : 'rotate(0deg)';
        childRows.forEach(child => {
            child.style.display = expanded ? '' : 'none';
        });
    });
}

async function retryAuditLogEntry(log) {
    // Match the failed audit log entry to a sync retry action
    try {
        if (log.action === 'code_sync_failed' && log.lock_id && log.slot_number) {
            // Find the lock entity_id from our state
            const lock = state.locks.find(l => l.id === log.lock_id);
            if (lock) {
                const result = await api(`/sync-status/retry/${lock.entity_id}/${log.slot_number}`, { method: 'POST' });
                if (result.success) {
                    showToast('Retry succeeded', 'success');
                    return true;
                }
                showToast(`Retry failed: ${result.error}`, 'error');
                return false;
            }
        }
        // For other failed actions, try retry-all as fallback
        const result = await api('/sync-status/retry-all', { method: 'POST' });
        const succeeded = result.results ? result.results.filter(r => r.success).length : 0;
        if (succeeded > 0) {
            showToast(`Retry: ${succeeded} succeeded`, 'success');
            return true;
        }
        showToast('No retryable operations found', 'info');
        return false;
    } catch (error) {
        showToast('Retry failed: ' + error.message, 'error');
        return false;
    }
}

function getActionLabel(action) {
    const labels = {
        'code_activated': 'Code Activated',
        'code_deactivated': 'Code Deactivated',
        'code_finalized': 'Code Finalized',
        'code_sync_failed': 'Sync Failed',
        'master_code_set': 'Master Code Set',
        'emergency_code_randomized': 'Emergency Randomized',
        'emergency_code_set': 'Emergency Code Set',
        'clear_all_codes': 'All Codes Cleared',
        'set_slot_code': 'Slot Code Set',
        'clear_slot_code': 'Slot Code Cleared',
        'code_disabled': 'Code Disabled',
        'code_enabled': 'Code Enabled',
        'booking_code_set': 'Booking Code Set',
        'booking_recoded': 'Booking Recoded',
        'auto_lock_changed': 'Auto-Lock Changed',
        'auto_lock_enable': 'Auto-Lock Enabled',
        'auto_lock_disable': 'Auto-Lock Disabled',
        'whole_house_unlock': 'Whole House Unlock',
        'whole_house_lock': 'Whole House Lock',
        'no_code_warning': 'No Code Warning',
    };
    return labels[action] || action.replace(/_/g, ' ');
}

function formatLogDetails(log) {
    // Use details field if it's meaningful (not just "Booking: <uid>")
    if (log.details && !log.details.startsWith('Booking: ') && log.details !== '—') {
        return log.details;
    }
    // Fall back to booking info from the API
    if (log.booking) {
        const b = log.booking;
        const ci = new Date(b.check_in + 'T00:00:00');
        const co = new Date(b.check_out + 'T00:00:00');
        const fmtDate = (d) => d.toLocaleDateString('en-GB', { month: 'short', day: 'numeric' });
        const parts = [b.guest_name];
        if (b.calendar_name) parts.push(b.calendar_name);
        parts.push(`${fmtDate(ci)}\u2013${fmtDate(co)}`);
        return parts.join(' \u00b7 ');
    }
    // Fall back to error message or empty
    return log.error_message || '—';
}

function getBatchLabel(logs) {
    if (!logs.length) return 'Batch Operation';
    const action = logs[0].action;
    const count = logs.length;
    const lockWord = count === 1 ? 'lock' : 'locks';
    const allOk = logs.every(l => l.success);
    const failCount = logs.filter(l => !l.success).length;
    const statusSuffix = allOk ? '' : ` · ${failCount} failed`;

    if (action === 'emergency_code_randomized') return `Emergency Codes Randomized · ${count} ${lockWord}${statusSuffix}`;
    if (action === 'auto_lock_enable') return `Auto-Lock Enabled · ${count} ${lockWord}${statusSuffix}`;
    if (action === 'auto_lock_disable') return `Auto-Lock Disabled · ${count} ${lockWord}${statusSuffix}`;
    if (action === 'whole_house_unlock') return `Whole House Unlock · ${count} ${lockWord}${statusSuffix}`;
    if (action === 'whole_house_lock') return `Whole House Lock · ${count} ${lockWord}${statusSuffix}`;
    if (action === 'code_activated') return `Codes Activated · ${count} ${lockWord}${statusSuffix}`;
    if (action === 'code_deactivated') return `Codes Deactivated · ${count} ${lockWord}${statusSuffix}`;

    return `${getActionLabel(action)} · ${count} ${lockWord}${statusSuffix}`;
}

function groupLogs(logs) {
    // Group logs by batch_id (explicit) or by same action + booking within 60s (implicit)
    const groups = [];
    const used = new Set();

    // First pass: group by batch_id
    const batchMap = {};
    logs.forEach((log, idx) => {
        if (log.batch_id) {
            if (!batchMap[log.batch_id]) batchMap[log.batch_id] = [];
            batchMap[log.batch_id].push({ log, idx });
        }
    });
    for (const [batchId, entries] of Object.entries(batchMap)) {
        if (entries.length > 1) {
            groups.push({
                type: 'group',
                logs: entries.map(e => e.log),
                timestamp: entries[0].log.timestamp,
            });
            entries.forEach(e => used.add(e.idx));
        }
    }

    // Second pass: group by same action + same booking details within 60s (no batch_id)
    for (let i = 0; i < logs.length; i++) {
        if (used.has(i)) continue;
        const log = logs[i];
        // Only try to group scheduler-fired actions (activate/deactivate)
        if (!log.batch_id && (log.action === 'code_activated' || log.action === 'code_deactivated')) {
            const cluster = [log];
            const clusterIdxs = [i];
            const t0 = new Date(log.timestamp).getTime();
            const details0 = log.details || '';
            for (let j = i + 1; j < logs.length; j++) {
                if (used.has(j)) continue;
                const other = logs[j];
                if (other.action !== log.action) continue;
                if (other.batch_id) continue;
                const tOther = new Date(other.timestamp).getTime();
                if (Math.abs(tOther - t0) > 120000) continue;  // within 2 minutes
                const detailsOther = other.details || '';
                if (details0 && detailsOther && details0 === detailsOther) {
                    cluster.push(other);
                    clusterIdxs.push(j);
                }
            }
            if (cluster.length > 1) {
                groups.push({
                    type: 'group',
                    logs: cluster,
                    timestamp: cluster[0].timestamp,
                });
                clusterIdxs.forEach(idx => used.add(idx));
                continue;
            }
        }
        if (!used.has(i)) {
            groups.push({ type: 'single', log, timestamp: log.timestamp });
            used.add(i);
        }
    }

    // Sort by timestamp descending (groups use first entry timestamp)
    groups.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    return groups;
}

function formatActivityTime(d) {
    const now = new Date();
    const diffMs = now - d;
    const diffMins = Math.floor(diffMs / 60000);

    if (diffMins < 1) return 'just now';
    if (diffMins < 60) return `${diffMins}m ago`;

    const diffHours = Math.floor(diffMins / 60);
    if (diffHours < 24) return `${diffHours}h ago`;

    const diffDays = Math.floor(diffHours / 24);
    if (diffDays < 7) return `${diffDays}d ago`;

    return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

function renderFailedSlotRow(slot) {
    const row = document.createElement('div');
    row.style.cssText = 'display:flex;align-items:center;gap:0.5rem;padding:0.3rem 0.4rem;margin-bottom:0.25rem;background:var(--danger-bg,rgba(220,53,69,0.08));border:1px solid var(--danger-border,rgba(220,53,69,0.2));border-radius:4px;font-size:0.75rem';

    const lockName = slot.lock_entity_id.replace('lock.', '').replace(/_/g, ' ');
    const label = getSlotLabel(slot.slot_number);

    const info = document.createElement('div');
    info.style.flex = '1';

    const lockSpan = document.createElement('strong');
    lockSpan.textContent = lockName;
    info.appendChild(lockSpan);
    info.appendChild(document.createTextNode(` slot ${slot.slot_number}`));

    if (label) {
        const labelSpan = document.createElement('span');
        labelSpan.style.color = 'var(--text-secondary)';
        labelSpan.textContent = ` (${label})`;
        info.appendChild(labelSpan);
    }
    if (slot.guest_name) {
        info.appendChild(document.createTextNode(` — ${slot.guest_name}`));
    }

    const errorLine = document.createElement('div');
    errorLine.style.color = 'var(--text-secondary)';
    errorLine.style.fontSize = '0.65rem';
    errorLine.textContent = `${slot.error || 'Unknown error'} · ${slot.retry_count} retries`;
    info.appendChild(errorLine);

    row.appendChild(info);

    const retryBtn = document.createElement('button');
    retryBtn.className = 'btn btn-sm btn-secondary';
    retryBtn.style.cssText = 'font-size:0.65rem;padding:0.15rem 0.4rem;white-space:nowrap';
    retryBtn.textContent = 'Retry';
    retryBtn.addEventListener('click', () => retrySyncSlot(slot.lock_entity_id, slot.slot_number, row, retryBtn));
    row.appendChild(retryBtn);

    return row;
}

function renderFailedOpRow(op) {
    const row = document.createElement('div');
    row.style.cssText = 'display:flex;align-items:center;gap:0.5rem;padding:0.3rem 0.4rem;margin-bottom:0.25rem;background:var(--warning-bg,rgba(255,193,7,0.08));border:1px solid var(--warning-border,rgba(255,193,7,0.3));border-radius:4px;font-size:0.75rem';

    const info = document.createElement('div');
    info.style.flex = '1';

    const lockSpan = document.createElement('strong');
    lockSpan.textContent = op.lock_name || op.lock_entity_id.replace('lock.', '').replace(/_/g, ' ');
    info.appendChild(lockSpan);

    const actionBadge = document.createElement('span');
    actionBadge.style.cssText = 'margin-left:0.4rem;padding:0.1rem 0.3rem;border-radius:3px;font-size:0.65rem;background:var(--bg-tertiary);color:var(--text-secondary)';
    actionBadge.textContent = op.action;
    info.appendChild(actionBadge);

    const errorLine = document.createElement('div');
    errorLine.style.color = 'var(--text-secondary)';
    errorLine.style.fontSize = '0.65rem';
    errorLine.textContent = `${op.error || 'Unknown error'} · ${op.retry_count} retries`;
    info.appendChild(errorLine);

    if (op.reason) {
        const reasonLine = document.createElement('div');
        reasonLine.style.color = 'var(--text-secondary)';
        reasonLine.style.fontSize = '0.6rem';
        reasonLine.style.fontStyle = 'italic';
        reasonLine.textContent = op.reason;
        info.appendChild(reasonLine);
    }

    row.appendChild(info);

    const btnGroup = document.createElement('div');
    btnGroup.style.cssText = 'display:flex;gap:0.25rem';

    const retryBtn = document.createElement('button');
    retryBtn.className = 'btn btn-sm btn-secondary';
    retryBtn.style.cssText = 'font-size:0.65rem;padding:0.15rem 0.4rem;white-space:nowrap';
    retryBtn.textContent = 'Retry';
    retryBtn.addEventListener('click', () => retryFailedOp(op.id, row, retryBtn));
    btnGroup.appendChild(retryBtn);

    const dismissBtn = document.createElement('button');
    dismissBtn.className = 'btn btn-sm btn-secondary';
    dismissBtn.style.cssText = 'font-size:0.65rem;padding:0.15rem 0.3rem;white-space:nowrap;opacity:0.6';
    dismissBtn.textContent = '\u2715';
    dismissBtn.title = 'Dismiss';
    dismissBtn.addEventListener('click', () => dismissFailedOp(op.id));
    btnGroup.appendChild(dismissBtn);

    row.appendChild(btnGroup);

    return row;
}

async function retrySyncSlot(lockEntityId, slotNumber, row, retryBtn) {
    try {
        retryBtn.disabled = true;
        retryBtn.textContent = '...';
        const result = await api(`/sync-status/retry/${lockEntityId}/${slotNumber}`, {
            method: 'POST',
        });
        if (result.success) {
            showToast('Retry succeeded', 'success');
            row.style.cssText = row.style.cssText.replace(/background:[^;]+/, 'background:rgba(40,167,69,0.1)');
            row.style.borderColor = 'rgba(40,167,69,0.3)';
            retryBtn.remove();
            await loadSyncStatus();
        } else {
            showToast(`Retry failed: ${result.error}`, 'error');
            retryBtn.disabled = false;
            retryBtn.textContent = 'Retry';
        }
    } catch (error) {
        showToast('Retry failed: ' + error.message, 'error');
        retryBtn.disabled = false;
        retryBtn.textContent = 'Retry';
    }
}

async function retryFailedOp(opId, row, retryBtn) {
    try {
        retryBtn.disabled = true;
        retryBtn.textContent = '...';
        const result = await api(`/sync-status/retry-op/${opId}`, { method: 'POST' });
        if (result.success) {
            showToast('Retry succeeded', 'success');
            row.style.cssText = row.style.cssText.replace(/background:[^;]+/, 'background:rgba(40,167,69,0.1)');
            row.style.borderColor = 'rgba(40,167,69,0.3)';
            retryBtn.parentElement.remove();
            await loadSyncStatus();
        } else {
            showToast(`Retry failed: ${result.error}`, 'error');
            retryBtn.disabled = false;
            retryBtn.textContent = 'Retry';
        }
    } catch (error) {
        showToast('Retry failed: ' + error.message, 'error');
        retryBtn.disabled = false;
        retryBtn.textContent = 'Retry';
    }
}

async function dismissFailedOp(opId) {
    try {
        await api(`/sync-status/dismiss-op/${opId}`, { method: 'POST' });
        await loadSyncStatus();
    } catch (error) {
        showToast('Dismiss failed: ' + error.message, 'error');
    }
}

async function retryAllFailed() {
    try {
        const result = await api('/sync-status/retry-all', { method: 'POST' });
        const succeeded = result.results.filter(r => r.success).length;
        showToast(`Retried ${result.retried}: ${succeeded} succeeded`, 'success');
        await loadSyncStatus();
    } catch (error) {
        showToast('Retry failed: ' + error.message, 'error');
    }
}

// Action Functions
async function lockAction(entityId, action) {
    try {
        await api(`/locks/${entityId}/action`, {
            method: 'POST',
            body: JSON.stringify({ action }),
        });
        showToast(`Lock ${action}ed successfully`, 'success');
    } catch (error) {
        showToast(error.message, 'error');
    }
}

async function loadCodesView() {
    loadEmergencyCodes();
    // Show current master code from locks data
    if (state.locks.length === 0) {
        state.locks = await api('/locks');
    }
    const masterCodes = [...new Set(state.locks.map(l => l.master_code).filter(Boolean))];
    const currentDiv = document.getElementById('master-code-current');
    if (currentDiv) {
        if (masterCodes.length === 1) {
            currentDiv.textContent = `Current: ${masterCodes[0]}`;
        } else if (masterCodes.length > 1) {
            currentDiv.textContent = `Current: mixed (${masterCodes.join(', ')})`;
        } else {
            currentDiv.textContent = 'Current: not set';
        }
    }
}

async function setMasterCode() {
    const code = document.getElementById('master-code-input').value;

    if (!code || !/^\d{4}$/.test(code)) {
        showToast('Code must be 4 digits', 'error');
        return;
    }

    try {
        const result = await api('/codes/master', {
            method: 'POST',
            body: JSON.stringify({ code }),
        });
        showToast(`Master code set on ${result.success_count}/${result.total_locks} locks`, 'success');
        state.locks = await api('/locks');
        loadCodesView();
    } catch (error) {
        showToast(error.message, 'error');
    }
}

async function loadEmergencyCodes() {
    try {
        state.emergencyCodes = await api('/codes/emergency');
        renderEmergencyCodes();
    } catch (error) {
        showToast('Failed to load emergency codes', 'error');
    }
}

function renderEmergencyCodes() {
    const container = document.getElementById('emergency-codes-list');
    if (!container) return;
    container.textContent = '';

    const codes = state.emergencyCodes || [];

    const table = document.createElement('table');
    table.style.width = '100%';

    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    ['Lock', 'Type', 'Emergency Code', ''].forEach(text => {
        const th = document.createElement('th');
        th.textContent = text;
        th.style.textAlign = 'left';
        th.style.padding = '0.5rem 0.75rem';
        headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);
    table.appendChild(thead);

    const tbody = document.createElement('tbody');
    codes.forEach(lock => {
        const row = document.createElement('tr');

        const nameCell = document.createElement('td');
        nameCell.textContent = lock.lock_name;
        nameCell.style.padding = '0.5rem 0.75rem';
        row.appendChild(nameCell);

        const typeCell = document.createElement('td');
        typeCell.style.padding = '0.5rem 0.75rem';
        const typeBadge = document.createElement('span');
        typeBadge.className = 'badge badge-info';
        typeBadge.textContent = lock.lock_type;
        typeCell.appendChild(typeBadge);
        row.appendChild(typeCell);

        const codeCell = document.createElement('td');
        codeCell.style.padding = '0.5rem 0.75rem';
        const codeInput = document.createElement('input');
        codeInput.type = 'text';
        codeInput.className = 'form-input';
        codeInput.style.width = '80px';
        codeInput.style.fontSize = '1rem';
        codeInput.style.fontFamily = 'monospace';
        codeInput.style.textAlign = 'center';
        codeInput.maxLength = 5;
        codeInput.value = lock.emergency_code || '-----';
        codeInput.dataset.lockId = lock.lock_id;
        codeCell.appendChild(codeInput);
        row.appendChild(codeCell);

        const actionCell = document.createElement('td');
        actionCell.style.padding = '0.5rem 0.75rem';
        const saveBtn = document.createElement('button');
        saveBtn.className = 'btn btn-sm btn-primary';
        saveBtn.textContent = 'Save';
        saveBtn.addEventListener('click', () => {
            saveEmergencyCode(lock.lock_id, codeInput.value);
        });
        actionCell.appendChild(saveBtn);
        row.appendChild(actionCell);

        tbody.appendChild(row);
    });

    table.appendChild(tbody);
    container.appendChild(table);
}

async function randomizeEmergencyCodes() {
    try {
        const result = await api('/codes/emergency/randomize', { method: 'POST' });
        showToast(`Randomized codes on ${result.success_count}/${result.total_locks} locks`, 'success');
        loadEmergencyCodes();
        loadLocks();
    } catch (error) {
        showToast(error.message, 'error');
    }
}

async function saveEmergencyCode(lockId, code) {
    if (!code || !/^\d{4,5}$/.test(code)) {
        showToast('Code must be 4-5 digits', 'error');
        return;
    }
    try {
        await api('/codes/emergency', {
            method: 'POST',
            body: JSON.stringify({ lock_id: lockId, code }),
        });
        showToast('Emergency code saved', 'success');
        loadLocks();
    } catch (error) {
        showToast(error.message, 'error');
    }
}

async function updateCalendarEntity(calendarId) {
    const entityId = document.getElementById(`entity-${calendarId}`).value.trim();

    try {
        await api(`/calendars/${calendarId}/entity`, {
            method: 'PUT',
            body: JSON.stringify({ ha_entity_id: entityId }),
        });
        showToast('Calendar entity updated', 'success');
        loadCalendars();
    } catch (error) {
        showToast(error.message, 'error');
    }
}

async function setBookingCode(bookingId) {
    const code = prompt('Enter new 4-digit PIN code for this guest:');
    if (!code) return;
    if (!/^\d{4,8}$/.test(code)) {
        showToast('Code must be 4-8 digits', 'error');
        return;
    }
    try {
        const result = await api(`/bookings/${bookingId}/set-code`, {
            method: 'POST',
            body: JSON.stringify({ code }),
        });
        let msg = `Code set to ${code}`;
        if (result.locks_updated) msg += ` (${result.locks_updated} lock(s) updated)`;
        if (result.locks_rescheduled) msg += ` (${result.locks_rescheduled} rescheduled)`;
        showToast(msg, 'success');
        loadBookings();
    } catch (error) {
        showToast(error.message, 'error');
    }
}

async function updateCalendarUrl(calendarId) {
    const url = document.getElementById(`url-${calendarId}`).value;

    try {
        await api(`/calendars/${calendarId}/url`, {
            method: 'PUT',
            body: JSON.stringify({ calendar_id: calendarId, ical_url: url }),
        });
        showToast('Calendar URL updated', 'success');
        loadCalendars();
    } catch (error) {
        showToast(error.message, 'error');
    }
}

async function refreshCalendars() {
    try {
        await api('/calendars/refresh', { method: 'POST' });
        showToast('Calendars refreshed', 'success');
        loadCalendars();
        loadBookings();
    } catch (error) {
        showToast(error.message, 'error');
    }
}

async function syncCalendarsToHA() {
    if (!confirm(
        'Sync all calendar iCal URLs to Home Assistant remote_calendar integrations?\n\n'
        + 'This will delete and re-create each remote_calendar config entry with the current URL. '
        + 'Entity IDs will be preserved. This may take a minute.'
    )) return;

    showToast('Syncing calendars to HA... this may take a minute', 'info');

    try {
        const result = await api('/calendars/sync-to-ha', { method: 'POST' });
        let msg = `Synced ${result.synced.length} calendar(s) to HA`;
        if (result.created.length > 0) {
            msg += `, ${result.created.length} newly created`;
        }
        if (result.errors.length > 0) {
            msg += `, ${result.errors.length} error(s)`;
            showToast(msg, 'error');
        } else {
            showToast(msg, 'success');
        }
        loadCalendars();
    } catch (error) {
        showToast('Sync failed: ' + error.message, 'error');
    }
}

// Store the original lock times for comparison when saving
let _originalLockTimes = [];

async function showTimeOverride(bookingId) {
    const booking = state.bookings.find(b => b.id === bookingId);
    if (!booking) return;

    document.getElementById('time-override-booking').textContent =
        `${booking.guest_name} — Code: ${booking.code || '---'} (${booking.check_in_date} to ${booking.check_out_date})`;
    document.getElementById('time-override-booking-id').value = bookingId;
    document.getElementById('time-override-notes').value = '';

    const container = document.getElementById('time-override-locks');
    container.textContent = '';

    // Show loading
    const loading = document.createElement('p');
    loading.textContent = 'Loading lock times...';
    loading.style.color = 'var(--text-secondary)';
    container.appendChild(loading);

    showModal('time-override-modal');

    try {
        const lockTimes = await api(`/bookings/${bookingId}/lock-times`);
        _originalLockTimes = lockTimes;
        container.textContent = '';

        if (lockTimes.length === 0) {
            const msg = document.createElement('p');
            msg.textContent = 'No locks associated with this booking.';
            msg.style.color = 'var(--text-secondary)';
            container.appendChild(msg);
            return;
        }

        // Pre-fill notes from first override that has one
        const existingNote = lockTimes.find(lt => lt.override_notes);
        if (existingNote) {
            document.getElementById('time-override-notes').value = existingNote.override_notes;
        }

        // Table header
        const table = document.createElement('table');
        table.style.width = '100%';
        table.style.fontSize = '0.85rem';
        const thead = document.createElement('thead');
        const headerRow = document.createElement('tr');
        ['Lock', 'Type', 'Activates', 'Deactivates', ''].forEach(text => {
            const th = document.createElement('th');
            th.textContent = text;
            th.style.textAlign = 'left';
            th.style.padding = '0.4rem 0.3rem';
            headerRow.appendChild(th);
        });
        thead.appendChild(headerRow);
        table.appendChild(thead);

        const tbody = document.createElement('tbody');

        lockTimes.forEach(lt => {
            const row = document.createElement('tr');
            row.dataset.lockId = lt.lock_id;

            // Lock name
            const nameCell = document.createElement('td');
            nameCell.textContent = lt.lock_name;
            nameCell.style.padding = '0.4rem 0.3rem';
            nameCell.style.whiteSpace = 'nowrap';
            row.appendChild(nameCell);

            // Lock type
            const typeCell = document.createElement('td');
            const typeBadge = document.createElement('span');
            typeBadge.className = 'badge badge-info';
            typeBadge.textContent = lt.lock_type;
            typeCell.style.padding = '0.4rem 0.3rem';
            typeCell.appendChild(typeBadge);
            row.appendChild(typeCell);

            // Activate input
            const activateCell = document.createElement('td');
            activateCell.style.padding = '0.4rem 0.3rem';
            const activateInput = document.createElement('input');
            activateInput.type = 'datetime-local';
            activateInput.className = 'form-input';
            activateInput.style.fontSize = '0.8rem';
            activateInput.style.padding = '0.25rem 0.4rem';
            activateInput.dataset.field = 'activate';
            activateInput.dataset.lockId = lt.lock_id;
            activateInput.value = toLocalDatetimeValue(lt.effective_activate);
            activateCell.appendChild(activateInput);
            row.appendChild(activateCell);

            // Deactivate input
            const deactivateCell = document.createElement('td');
            deactivateCell.style.padding = '0.4rem 0.3rem';
            const deactivateInput = document.createElement('input');
            deactivateInput.type = 'datetime-local';
            deactivateInput.className = 'form-input';
            deactivateInput.style.fontSize = '0.8rem';
            deactivateInput.style.padding = '0.25rem 0.4rem';
            deactivateInput.dataset.field = 'deactivate';
            deactivateInput.dataset.lockId = lt.lock_id;
            deactivateInput.value = toLocalDatetimeValue(lt.effective_deactivate);
            deactivateCell.appendChild(deactivateInput);
            row.appendChild(deactivateCell);

            // Override indicator
            const statusCell = document.createElement('td');
            statusCell.style.padding = '0.4rem 0.3rem';
            if (lt.has_override) {
                const badge = document.createElement('span');
                badge.className = 'badge badge-warning';
                badge.textContent = 'Override';
                badge.style.fontSize = '0.7rem';
                statusCell.appendChild(badge);
            }
            row.appendChild(statusCell);

            tbody.appendChild(row);
        });

        table.appendChild(tbody);
        container.appendChild(table);
    } catch (error) {
        container.textContent = '';
        const errMsg = document.createElement('p');
        errMsg.textContent = 'Error loading lock times: ' + error.message;
        errMsg.style.color = 'var(--accent-red)';
        container.appendChild(errMsg);
    }
}

function toLocalDatetimeValue(isoString) {
    // Convert ISO string to datetime-local input value (YYYY-MM-DDTHH:MM)
    if (!isoString) return '';
    const d = new Date(isoString);
    const year = d.getFullYear();
    const month = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    const hours = String(d.getHours()).padStart(2, '0');
    const minutes = String(d.getMinutes()).padStart(2, '0');
    return `${year}-${month}-${day}T${hours}:${minutes}`;
}

async function saveAllTimeOverrides() {
    const bookingId = parseInt(document.getElementById('time-override-booking-id').value);
    const notes = document.getElementById('time-override-notes').value;
    const container = document.getElementById('time-override-locks');

    let savedCount = 0;
    let errorCount = 0;

    for (const lt of _originalLockTimes) {
        const activateInput = container.querySelector(`input[data-field="activate"][data-lock-id="${lt.lock_id}"]`);
        const deactivateInput = container.querySelector(`input[data-field="deactivate"][data-lock-id="${lt.lock_id}"]`);

        if (!activateInput || !deactivateInput) continue;

        const newActivate = activateInput.value;
        const newDeactivate = deactivateInput.value;

        // Check if values changed from the effective times
        const origActivate = toLocalDatetimeValue(lt.effective_activate);
        const origDeactivate = toLocalDatetimeValue(lt.effective_deactivate);

        if (newActivate === origActivate && newDeactivate === origDeactivate) {
            continue; // No change for this lock
        }

        try {
            await api(`/bookings/${bookingId}/time-override`, {
                method: 'POST',
                body: JSON.stringify({
                    booking_id: bookingId,
                    lock_id: lt.lock_id,
                    activate_at: newActivate ? new Date(newActivate).toISOString() : null,
                    deactivate_at: newDeactivate ? new Date(newDeactivate).toISOString() : null,
                    notes: notes || null,
                }),
            });
            savedCount++;
        } catch (error) {
            errorCount++;
        }
    }

    if (errorCount > 0) {
        showToast(`Saved ${savedCount} overrides, ${errorCount} failed`, 'error');
    } else if (savedCount > 0) {
        showToast(`Saved ${savedCount} time override(s)`, 'success');
    } else {
        showToast('No changes to save', 'info');
    }
    closeModal('time-override-modal');
}

function showLockDetail(entityId) {
    const lock = state.locks.find(l => l.entity_id === entityId);
    if (!lock) return;

    state.selectedLock = lock;

    document.getElementById('lock-detail-title').textContent = lock.name;
    document.getElementById('lock-detail-entity').textContent = lock.entity_id;

    renderLockDetailControls(lock);
    renderLockDetailSlots(lock);
    showLockTab('slots');

    const clearAllBtn = document.getElementById('clear-all-codes-btn');
    clearAllBtn.onclick = () => clearAllCodes(lock.entity_id);

    showModal('lock-detail-modal');
}

async function toggleAutoLock(entityId, enabled) {
    try {
        await api(`/locks/${entityId}/auto-lock`, {
            method: 'POST',
            body: JSON.stringify({ enabled }),
        });
        showToast(`Auto-lock ${enabled ? 'enabled' : 'disabled'}`, 'success');
        await refreshLockDetail(entityId);
    } catch (error) {
        showToast(error.message, 'error');
    }
}

function renderLockDetailControls(lock) {
    const controls = document.getElementById('lock-detail-controls');
    controls.textContent = '';

    const autoLockLabel = document.createElement('span');
    autoLockLabel.textContent = 'Auto-Lock:';
    autoLockLabel.style.fontWeight = '600';
    controls.appendChild(autoLockLabel);

    const autoLockOnBtn = document.createElement('button');
    autoLockOnBtn.className = `btn btn-sm ${lock.auto_lock_enabled === true ? 'btn-success' : 'btn-secondary'}`;
    autoLockOnBtn.textContent = 'On';
    autoLockOnBtn.style.fontSize = '0.7rem';
    autoLockOnBtn.style.padding = '0.15rem 0.5rem';
    autoLockOnBtn.addEventListener('click', () => toggleAutoLock(lock.entity_id, true));

    const autoLockOffBtn = document.createElement('button');
    autoLockOffBtn.className = `btn btn-sm ${lock.auto_lock_enabled === false ? 'btn-danger' : 'btn-secondary'}`;
    autoLockOffBtn.textContent = 'Off';
    autoLockOffBtn.style.fontSize = '0.7rem';
    autoLockOffBtn.style.padding = '0.15rem 0.5rem';
    autoLockOffBtn.addEventListener('click', () => toggleAutoLock(lock.entity_id, false));

    controls.appendChild(autoLockOnBtn);
    controls.appendChild(autoLockOffBtn);

    // Active codes summary
    const activeCount = countActiveCodes(lock);
    const activeSummary = document.createElement('span');
    activeSummary.style.marginLeft = 'auto';
    activeSummary.style.color = 'var(--text-secondary)';
    activeSummary.textContent = `${activeCount} active code${activeCount !== 1 ? 's' : ''}`;
    controls.appendChild(activeSummary);
}

function renderLockDetailSlots(lock) {
    const slotsContainer = document.getElementById('lock-detail-slots');
    slotsContainer.textContent = '';

    lock.slots.forEach(slot => {
        // Determine the effective code based on slot type
        let effectiveCode = slot.assigned_code || slot.current_code || '';
        if (slot.slot_number === 1 && lock.master_code) effectiveCode = lock.master_code;
        if (slot.slot_number === 20 && lock.emergency_code) effectiveCode = lock.emergency_code;
        const hasCode = !!effectiveCode;

        const slotDiv = document.createElement('div');
        slotDiv.className = `code-slot ${hasCode ? 'active' : 'empty'}`;

        const topRow = document.createElement('div');
        topRow.style.display = 'flex';
        topRow.style.justifyContent = 'space-between';
        topRow.style.alignItems = 'center';
        topRow.style.marginBottom = '0.25rem';

        const numDiv = document.createElement('div');
        numDiv.className = 'code-slot-number';
        numDiv.textContent = slot.slot_number;

        const labelDiv = document.createElement('div');
        labelDiv.style.fontSize = '0.625rem';
        labelDiv.style.color = 'var(--text-secondary)';
        labelDiv.textContent = getSlotLabel(slot.slot_number);

        topRow.appendChild(numDiv);
        topRow.appendChild(labelDiv);

        // Show guest assignment info if present
        if (slot.guest_name) {
            const guestDiv = document.createElement('div');
            guestDiv.style.fontSize = '0.7rem';
            guestDiv.style.padding = '0.2rem 0.3rem';
            guestDiv.style.marginBottom = '0.25rem';
            guestDiv.style.borderRadius = '3px';
            guestDiv.style.background = slot.is_active
                ? 'var(--success-bg, rgba(40,167,69,0.1))'
                : 'var(--warning-bg, rgba(255,193,7,0.1))';
            guestDiv.style.border = `1px solid ${slot.is_active
                ? 'var(--success-border, rgba(40,167,69,0.3))'
                : 'var(--warning-border, rgba(255,193,7,0.3))'}`;

            const nameSpan = document.createElement('div');
            nameSpan.style.fontWeight = '600';
            nameSpan.style.fontSize = '0.7rem';
            nameSpan.textContent = slot.guest_name;
            guestDiv.appendChild(nameSpan);

            if (slot.check_in && slot.check_out) {
                const dateSpan = document.createElement('div');
                dateSpan.style.fontSize = '0.6rem';
                dateSpan.style.color = 'var(--text-secondary)';
                const ci = new Date(slot.check_in).toLocaleDateString('en-GB', { day: 'numeric', month: 'short' });
                const co = new Date(slot.check_out).toLocaleDateString('en-GB', { day: 'numeric', month: 'short' });
                dateSpan.textContent = `${ci} → ${co}`;
                guestDiv.appendChild(dateSpan);
            }

            slotDiv.appendChild(topRow);
            slotDiv.appendChild(guestDiv);
        } else {
            slotDiv.appendChild(topRow);
        }

        const input = document.createElement('input');
        input.type = 'text';
        input.className = 'form-input';
        input.style.width = '100%';
        input.style.fontSize = '0.9rem';
        input.style.fontFamily = 'monospace';
        input.style.textAlign = 'center';
        input.style.padding = '0.2rem';
        input.style.marginBottom = '0.25rem';
        input.maxLength = 8;
        input.value = effectiveCode;
        input.placeholder = '----';

        const btnRow = document.createElement('div');
        btnRow.style.display = 'flex';
        btnRow.style.gap = '0.25rem';

        const setBtn = document.createElement('button');
        setBtn.className = 'btn btn-sm btn-primary';
        setBtn.style.flex = '1';
        setBtn.style.fontSize = '0.7rem';
        setBtn.style.padding = '0.15rem 0.3rem';
        setBtn.textContent = 'Set';
        setBtn.addEventListener('click', () => setSlotCode(lock.entity_id, slot.slot_number, input.value));

        const clearBtn = document.createElement('button');
        clearBtn.className = 'btn btn-sm btn-secondary';
        clearBtn.style.flex = '1';
        clearBtn.style.fontSize = '0.7rem';
        clearBtn.style.padding = '0.15rem 0.3rem';
        clearBtn.textContent = 'Clear';
        clearBtn.addEventListener('click', () => clearSlotCode(lock.entity_id, slot.slot_number));

        btnRow.appendChild(setBtn);
        btnRow.appendChild(clearBtn);

        slotDiv.appendChild(input);
        slotDiv.appendChild(btnRow);
        slotsContainer.appendChild(slotDiv);
    });
}

async function setSlotCode(entityId, slotNumber, code) {
    if (!code || !/^\d{4,8}$/.test(code)) {
        showToast('Code must be 4-8 digits', 'error');
        return;
    }
    try {
        await api(`/locks/${entityId}/slots/${slotNumber}/set`, {
            method: 'POST',
            body: JSON.stringify({ code }),
        });
        showToast(`Slot ${slotNumber} code set`, 'success');
        await refreshLockDetail(entityId);
    } catch (error) {
        showToast(error.message, 'error');
    }
}

async function clearSlotCode(entityId, slotNumber) {
    try {
        await api(`/locks/${entityId}/slots/${slotNumber}/clear`, { method: 'POST' });
        showToast(`Slot ${slotNumber} cleared`, 'success');
        await refreshLockDetail(entityId);
    } catch (error) {
        showToast(error.message, 'error');
    }
}

async function clearAllCodes(entityId) {
    if (!confirm('Clear ALL codes (including master and emergency) on this lock?')) return;
    try {
        const result = await api(`/locks/${entityId}/clear-all-codes`, { method: 'POST' });
        showToast(`Cleared ${result.cleared}/20 slots`, result.errors.length ? 'error' : 'success');
        await refreshLockDetail(entityId);
    } catch (error) {
        showToast(error.message, 'error');
    }
}

async function refreshLockDetail(entityId) {
    state.locks = await api('/locks');
    const lock = state.locks.find(l => l.entity_id === entityId);
    if (lock) {
        state.selectedLock = lock;
        renderLockDetailControls(lock);
        renderLockDetailSlots(lock);
    }
    renderLocks();
}

function showLockTab(tab) {
    const slotsTab = document.getElementById('lock-tab-slots');
    const historyTab = document.getElementById('lock-tab-history');
    const slotsBtn = document.getElementById('tab-slots-btn');
    const historyBtn = document.getElementById('tab-history-btn');

    if (tab === 'slots') {
        slotsTab.style.display = '';
        historyTab.style.display = 'none';
        slotsBtn.className = 'btn btn-sm btn-primary';
        historyBtn.className = 'btn btn-sm btn-secondary';
    } else {
        slotsTab.style.display = 'none';
        historyTab.style.display = '';
        slotsBtn.className = 'btn btn-sm btn-secondary';
        historyBtn.className = 'btn btn-sm btn-primary';
        if (state.selectedLock) {
            loadUnlockHistory(state.selectedLock.entity_id);
        }
    }
}

async function loadUnlockHistory(lockEntityId) {
    const container = document.getElementById('lock-history-content');
    container.textContent = '';

    const loading = document.createElement('div');
    loading.className = 'loading';
    const spinner = document.createElement('div');
    spinner.className = 'spinner';
    loading.appendChild(spinner);
    loading.appendChild(document.createTextNode(' Loading history...'));
    container.appendChild(loading);

    try {
        const events = await api(`/locks/${lockEntityId}/unlock-history?limit=50`);
        container.textContent = '';

        if (events.length === 0) {
            const empty = document.createElement('p');
            empty.textContent = 'No unlock events recorded yet.';
            empty.style.color = 'var(--text-secondary)';
            empty.style.textAlign = 'center';
            empty.style.padding = '2rem';
            container.appendChild(empty);
            return;
        }

        const table = document.createElement('table');
        table.style.width = '100%';
        table.style.fontSize = '0.85rem';

        const thead = document.createElement('thead');
        const headerRow = document.createElement('tr');
        ['Time', 'Guest', 'Slot', 'Method'].forEach(text => {
            const th = document.createElement('th');
            th.textContent = text;
            th.style.textAlign = 'left';
            th.style.padding = '0.4rem 0.5rem';
            th.style.fontSize = '0.8rem';
            headerRow.appendChild(th);
        });
        thead.appendChild(headerRow);
        table.appendChild(thead);

        const tbody = document.createElement('tbody');
        events.forEach(event => {
            const row = document.createElement('tr');

            const timeCell = document.createElement('td');
            timeCell.style.padding = '0.4rem 0.5rem';
            timeCell.style.whiteSpace = 'nowrap';
            const d = new Date(event.timestamp);
            timeCell.textContent = d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short' })
                + ' ' + d.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });
            row.appendChild(timeCell);

            const guestCell = document.createElement('td');
            guestCell.style.padding = '0.4rem 0.5rem';
            guestCell.textContent = event.guest_name || 'Unknown';
            if (!event.guest_name) guestCell.style.color = 'var(--text-secondary)';
            row.appendChild(guestCell);

            const slotCell = document.createElement('td');
            slotCell.style.padding = '0.4rem 0.5rem';
            if (event.slot_number != null) {
                slotCell.textContent = `#${event.slot_number}`;
                const label = getSlotLabel(event.slot_number);
                if (label) {
                    const labelSpan = document.createElement('span');
                    labelSpan.style.color = 'var(--text-secondary)';
                    labelSpan.style.fontSize = '0.75rem';
                    labelSpan.textContent = ` ${label}`;
                    slotCell.appendChild(labelSpan);
                }
            } else {
                slotCell.textContent = '---';
                slotCell.style.color = 'var(--text-secondary)';
            }
            row.appendChild(slotCell);

            const methodCell = document.createElement('td');
            methodCell.style.padding = '0.4rem 0.5rem';
            const methodBadge = document.createElement('span');
            methodBadge.className = 'badge badge-info';
            methodBadge.textContent = event.method;
            methodBadge.style.fontSize = '0.7rem';
            methodCell.appendChild(methodBadge);
            row.appendChild(methodCell);

            tbody.appendChild(row);
        });

        table.appendChild(tbody);
        container.appendChild(table);
    } catch (error) {
        container.textContent = '';
        const errMsg = document.createElement('p');
        errMsg.textContent = 'Error loading history: ' + error.message;
        errMsg.style.color = 'var(--accent-red)';
        container.appendChild(errMsg);
    }
}

function getSlotLabel(slotNumber) {
    const labels = {
        1: 'Master',
        2: 'Room 1', 3: 'Room 1',
        4: 'Room 2', 5: 'Room 2',
        6: 'Room 3', 7: 'Room 3',
        8: 'Room 4', 9: 'Room 4',
        10: 'Room 5', 11: 'Room 5',
        12: 'Room 6', 13: 'Room 6',
        14: 'Suite A', 15: 'Suite A',
        16: 'Suite B', 17: 'Suite B',
        18: 'Whole', 19: 'Whole',
        20: 'Emergency',
    };
    return labels[slotNumber] || '';
}

// Modal Functions
function showModal(modalId) {
    document.getElementById(modalId).classList.add('active');
}

function closeModal(modalId) {
    document.getElementById(modalId).classList.remove('active');
}

// Toast Notifications
function showToast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.textContent = message;
    container.appendChild(toast);

    setTimeout(() => {
        toast.remove();
    }, 5000);
}

// Navigation
function setView(view) {
    state.currentView = view;

    document.querySelectorAll('.nav-item').forEach(item => {
        item.classList.remove('active');
        if (item.dataset.view === view) {
            item.classList.add('active');
        }
    });

    document.querySelectorAll('.view-content').forEach(content => {
        content.style.display = content.id === `${view}-view` ? 'block' : 'none';
    });

    if (view === 'locks') loadLocks();
    if (view === 'bookings') loadBookings();
    if (view === 'calendars') loadCalendars();
    if (view === 'codes') loadCodesView();
    if (view === 'activity') loadActivityView();
}

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    // Set up navigation
    document.querySelectorAll('.nav-item').forEach(item => {
        item.addEventListener('click', () => setView(item.dataset.view));
    });

    // Close modals on overlay click
    document.querySelectorAll('.modal-overlay').forEach(overlay => {
        overlay.addEventListener('click', (e) => {
            if (e.target === overlay) {
                overlay.classList.remove('active');
            }
        });
    });

    // Load house info and initial view
    loadHouseInfo();
    setView('locks');
    loadSyncStatus();

    // Periodic sync status update
    setInterval(loadSyncStatus, 30000);
});
