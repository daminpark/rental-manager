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

        const statusDiv = document.createElement('div');
        statusDiv.className = 'lock-status locked';
        const statusDot = document.createElement('span');
        statusDot.className = 'lock-status-dot';
        statusDiv.appendChild(statusDot);
        statusDiv.appendChild(document.createTextNode(' Locked'));

        header.appendChild(nameDiv);
        header.appendChild(statusDiv);

        const info = document.createElement('div');
        info.className = 'lock-info';

        const infoItems = [
            { label: 'Type', value: lock.lock_type },
            { label: 'Active Codes', value: countActiveCodes(lock) },
            { label: 'Master Code', value: lock.master_code || '---' },
            { label: 'Emergency Code', value: lock.emergency_code || '---' },
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
    return lock.slots.filter(s => s.current_code && s.slot_number > 1 && s.slot_number < 20).length;
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
    const btn = document.createElement('button');
    btn.className = 'btn btn-sm btn-secondary';
    btn.textContent = 'Set Times';
    btn.addEventListener('click', (e) => {
        e.stopPropagation();
        showTimeOverride(booking.id);
    });
    actionsCell.appendChild(btn);
    row.appendChild(actionsCell);

    return row;
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

        const badge = document.createElement('span');
        badge.className = cal.last_fetch_error ? 'badge badge-danger' : 'badge badge-success';
        badge.textContent = cal.last_fetch_error ? 'Error' : 'OK';

        header.appendChild(title);
        header.appendChild(badge);

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

        const input = document.createElement('input');
        input.type = 'text';
        input.className = 'form-input';
        input.value = cal.ical_url || '';
        input.placeholder = 'iCal URL';
        input.id = `url-${cal.calendar_id}`;

        const updateBtn = document.createElement('button');
        updateBtn.className = 'btn btn-sm btn-primary';
        updateBtn.style.marginTop = '0.5rem';
        updateBtn.textContent = 'Update URL';
        updateBtn.addEventListener('click', () => updateCalendarUrl(cal.calendar_id));

        inputDiv.appendChild(input);
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
        codeInput.maxLength = 4;
        codeInput.value = lock.emergency_code || '----';
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
    if (!code || code.length !== 4 || !/^\d{4}$/.test(code)) {
        showToast('Code must be 4 digits', 'error');
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

    renderLockDetailSlots(lock);

    const clearAllBtn = document.getElementById('clear-all-codes-btn');
    clearAllBtn.onclick = () => clearAllCodes(lock.entity_id);

    showModal('lock-detail-modal');
}

function renderLockDetailSlots(lock) {
    const slotsContainer = document.getElementById('lock-detail-slots');
    slotsContainer.textContent = '';

    lock.slots.forEach(slot => {
        const slotDiv = document.createElement('div');
        slotDiv.className = `code-slot ${slot.current_code ? 'active' : 'empty'}`;

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
        input.value = slot.current_code || '';
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

        slotDiv.appendChild(topRow);
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
        renderLockDetailSlots(lock);
    }
    renderLocks();
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
