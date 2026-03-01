<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Hyne Pallets — Manufacturing Management</title>
  <script src="https://unpkg.com/react@18/umd/react.development.js" crossorigin></script>
  <script src="https://unpkg.com/react-dom@18/umd/react-dom.development.js" crossorigin></script>
  <script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://unpkg.com/lucide@latest/dist/umd/lucide.js"></script>
  <link rel="stylesheet" href="style.css" />
  <script>
    tailwind.config = {
      theme: {
        extend: {
          colors: {
            navy: '#07324C',
            red: { hp: '#ED1C24' },
            lightblue: '#ABBFC8',
          }
        }
      }
    }
  </script>
</head>
<body style="margin:0;padding:0;background:#F5F7FA;font-family:'Segoe UI',system-ui,sans-serif;">

<div id="root"></div>
<div id="toast-container"></div>

<script type="text/babel">
const { useState, useEffect, useRef, useCallback, createContext, useContext } = React;

// ═══════════════════════════════════════════════════════════════
// GLOBAL CONFIG
// ═══════════════════════════════════════════════════════════════
const API_BASE = "/api";
const localDateStr = (d = new Date()) => { const y=d.getFullYear(); const m=String(d.getMonth()+1).padStart(2,'0'); const dd=String(d.getDate()).padStart(2,'0'); return `${y}-${m}-${dd}`; };

// Sandbox compatibility stubs (not needed on Railway — always-on server)
let _sandboxReady = true;
let _sandboxListeners = [];
const _notifySandbox = (msg) => _sandboxListeners.forEach(fn => { try { fn(msg); } catch(e){} });
const onSandboxStatus = (fn) => { _sandboxListeners.push(fn); return () => { _sandboxListeners = _sandboxListeners.filter(f => f !== fn); }; };
const ensureSandbox = () => Promise.resolve(true);

const _buildUrl = (path, token) => {
  let routeUrl = `${API_BASE}${path}`;
  if (token) {
    const sep = routeUrl.includes('?') ? '&' : '?';
    routeUrl += `${sep}_token=${encodeURIComponent(token)}`;
  }
  return routeUrl;
};

const api = async (path, options = {}, token = null) => {
  const headers = { 'Content-Type': 'application/json', ...(options.headers || {}) };
  if (token) headers['Authorization'] = `Bearer ${token}`;
  const routeUrl = _buildUrl(path, token);
  const res = await fetch(routeUrl, { ...options, headers });
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { error: text }; }
  if (!res.ok) {
    throw new Error(data.error || `HTTP ${res.status}`);
  }
  return data;
};

// ═══════════════════════════════════════════════════════════════
// TOAST
// ═══════════════════════════════════════════════════════════════
const toast = (msg, type = 'info') => {
  const el = document.createElement('div');
  el.className = `toast toast-${type}`;
  el.textContent = msg;
  document.getElementById('toast-container').appendChild(el);
  setTimeout(() => el.remove(), 3500);
};

// ═══════════════════════════════════════════════════════════════
// STATUS HELPERS
// ═══════════════════════════════════════════════════════════════
const STATUS_LABELS = { T:'New/Tray', C:'Cut List', R:'Ready', P:'In Production', F:'Finished', dispatched:'Dispatched', delivered:'Delivered', collected:'Collected' };
const StatusBadge = ({ status }) => (
  <span className={`badge badge-${status}`}>{STATUS_LABELS[status] || status}</span>
);

const Spinner = () => <div className="spinner" />;
const Empty = ({ msg = 'No data found' }) => (
  <div className="flex flex-col items-center py-12 text-gray-400">
    <svg width="40" height="40" fill="none" stroke="currentColor" strokeWidth="1.5" viewBox="0 0 24 24"><rect x="3" y="3" width="18" height="18" rx="3"/><path d="M9 9h6M9 12h6M9 15h4"/></svg>
    <p className="mt-3 text-sm font-medium">{msg}</p>
  </div>
);

// Lucide icon helper
const Icon = ({ name, size = 18, className = '', strokeWidth = 1.8 }) => {
  const ref = useRef(null);
  useEffect(() => {
    try {
      if (ref.current && window.lucide) {
        // Try lucide icon creation
        const iconFn = window.lucide[name];
        if (typeof iconFn === 'function') {
          const result = iconFn([size, size], { class: className, 'stroke-width': strokeWidth });
          ref.current.innerHTML = '';
          if (typeof result === 'string') {
            ref.current.innerHTML = result;
          } else if (result instanceof SVGElement || result instanceof HTMLElement) {
            ref.current.innerHTML = '';
            ref.current.appendChild(result);
          } else if (Array.isArray(result) && result.length >= 2) {
            // Lucide returns [tag, attrs, children]
            const [tag, attrs, children] = result;
            const svg = document.createElementNS('http://www.w3.org/2000/svg', tag);
            if (attrs) Object.entries(attrs).forEach(([k,v]) => svg.setAttribute(k, v));
            svg.setAttribute('width', size);
            svg.setAttribute('height', size);
            svg.setAttribute('stroke', 'currentColor');
            svg.setAttribute('fill', 'none');
            svg.setAttribute('stroke-width', strokeWidth);
            if (children) children.forEach(([ct, ca]) => {
              const child = document.createElementNS('http://www.w3.org/2000/svg', ct);
              if (ca) Object.entries(ca).forEach(([k,v]) => child.setAttribute(k, v));
              svg.appendChild(child);
            });
            ref.current.innerHTML = '';
            ref.current.appendChild(svg);
          }
        } else if (window.lucide.icons && window.lucide.icons[name]) {
          // Alternative API: lucide.icons[name]
          const iconData = window.lucide.icons[name];
          const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
          svg.setAttribute('width', size);
          svg.setAttribute('height', size);
          svg.setAttribute('viewBox', '0 0 24 24');
          svg.setAttribute('fill', 'none');
          svg.setAttribute('stroke', 'currentColor');
          svg.setAttribute('stroke-width', strokeWidth);
          svg.setAttribute('stroke-linecap', 'round');
          svg.setAttribute('stroke-linejoin', 'round');
          if (Array.isArray(iconData)) {
            iconData.forEach(([tag, attrs]) => {
              const el = document.createElementNS('http://www.w3.org/2000/svg', tag);
              if (attrs) Object.entries(attrs).forEach(([k,v]) => el.setAttribute(k, v));
              svg.appendChild(el);
            });
          }
          ref.current.innerHTML = '';
          ref.current.appendChild(svg);
        }
      }
    } catch(e) { /* Silently fail - icon just won't show */ }
  }, [name, size]);
  return <span ref={ref} style={{ display: 'inline-flex', alignItems: 'center', flexShrink: 0, width: size, height: size }} />;
};

// ═══════════════════════════════════════════════════════════════
// LOGIN PAGE
// ═══════════════════════════════════════════════════════════════
const LoginPage = ({ onLogin }) => {
  const [tab, setTab] = useState('office');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [username, setUsername] = useState('');
  const [pin, setPin] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [floorUsers, setFloorUsers] = useState([]);
  const [warmupMsg, setWarmupMsg] = useState('Connecting to server...');
  const [sandboxOk, setSandboxOk] = useState(false);

  useEffect(() => {
    // Subscribe to sandbox status updates for live progress
    const unsub = onSandboxStatus(msg => setWarmupMsg(msg));
    // Pre-warm the sandbox while user sees the login form
    ensureSandbox().then(ok => {
      setSandboxOk(ok);
      if (ok) {
        setWarmupMsg('');
        api('/users').then(u => setFloorUsers(u.filter(x => x.role === 'floor_worker'))).catch(() => {});
      } else {
        setWarmupMsg('Server unavailable — please refresh the page');
      }
    });
    return unsub;
  }, []);

  const handleOfficeLogin = async (e) => {
    e.preventDefault();
    setLoading(true); setError('');
    try {
      const data = await api('/auth/login', { method: 'POST', body: JSON.stringify({ email, password }) });
      if (!data || !data.user) { setError('Login failed — invalid response from server'); setLoading(false); return; }
      onLogin(data.user, data.token);
    } catch (err) {
      setError(err.message || 'Login failed');
    }
    setLoading(false);
  };

  const handlePinLogin = async () => {
    if (!username) { setError('Please select a username'); return; }
    if (pin.length < 4) { setError('Enter at least 4 digits'); return; }
    setLoading(true); setError('');
    try {
      const data = await api('/auth/pin-login', { method: 'POST', body: JSON.stringify({ username, pin }) });
      if (!data || !data.user) { setError('Login failed — invalid response from server'); setLoading(false); return; }
      onLogin(data.user, data.token);
    } catch (err) { setError(err.message || 'Login failed'); setPin(''); }
    setLoading(false);
  };

  const PinKey = ({ val, label }) => (
    <button className="pin-key no-select" onClick={() => {
      if (val === 'del') setPin(p => p.slice(0,-1));
      else if (pin.length < 8) setPin(p => p + val);
    }}>
      {label || val}
    </button>
  );

  return (
    <div className="min-h-screen flex flex-col" style={{background:'linear-gradient(135deg,#07324C 0%,#0a4a6e 60%,#0d5a88 100%)'}}>
      {/* Header */}
      <div className="flex items-center gap-4 p-8">
        <div className="logo-chevron" style={{width:40,height:40}}></div>
        <div>
          <div className="text-white text-2xl font-black tracking-widest">HYNE PALLETS</div>
          <div className="text-lightblue text-xs tracking-wider mt-0.5">MANUFACTURING MANAGEMENT SYSTEM</div>
        </div>
      </div>

      {/* Card */}
      <div className="flex-1 flex items-center justify-center px-4 pb-12">
        <div className="bg-white rounded-2xl shadow-2xl w-full max-w-md overflow-hidden">
          {/* Tab bar */}
          <div className="flex">
            <button onClick={() => { setTab('office'); setError(''); }}
              className={`flex-1 py-4 text-sm font-bold transition-all ${tab==='office' ? 'bg-[#07324C] text-white' : 'bg-gray-50 text-gray-500 hover:bg-gray-100'}`}>
              Office Login
            </button>
            <button onClick={() => { setTab('floor'); setError(''); }}
              className={`flex-1 py-4 text-sm font-bold transition-all ${tab==='floor' ? 'bg-[#07324C] text-white' : 'bg-gray-50 text-gray-500 hover:bg-gray-100'}`}>
              Floor Login
            </button>
          </div>

          <div className="p-8">
            {warmupMsg && (
              <div className="mb-4 p-3 rounded-lg text-sm font-medium flex items-center gap-2" style={{background:'#eff6ff',border:'1px solid #bfdbfe',color:'#1e40af'}}>
                <svg className="animate-spin" style={{width:16,height:16}} viewBox="0 0 24 24" fill="none">
                  <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="3" opacity="0.3"/>
                  <path d="M12 2a10 10 0 0 1 10 10" stroke="currentColor" strokeWidth="3" strokeLinecap="round"/>
                </svg>
                {warmupMsg}
              </div>
            )}
            {error && <div className="mb-4 p-3 bg-red-50 border border-red-200 text-red-700 rounded-lg text-sm font-medium">{error}</div>}

            {tab === 'office' ? (
              <form onSubmit={handleOfficeLogin} className="space-y-4">
                <div>
                  <label className="hp-label">Email Address</label>
                  <input className="hp-input" type="email" value={email} onChange={e=>setEmail(e.target.value)} placeholder="you@hynepallets.com.au" required />
                </div>
                <div>
                  <label className="hp-label">Password</label>
                  <input className="hp-input" type="password" value={password} onChange={e=>setPassword(e.target.value)} placeholder="••••••••" required />
                </div>
                <button className="btn btn-primary w-full justify-center py-3 text-base mt-2" disabled={loading}>
                  {loading ? <Spinner /> : 'Sign In'}
                </button>
                <p className="text-xs text-gray-400 text-center mt-2">Demo: tim@hynepallets.com.au / admin123</p>
              </form>
            ) : (
              <div>
                <div className="mb-4">
                  <label className="hp-label">Worker Name</label>
                  <select className="hp-input" value={username} onChange={e=>setUsername(e.target.value)}>
                    <option value="">— Select your name —</option>
                    {floorUsers.map(u => <option key={u.id} value={u.username}>{u.full_name}</option>)}
                    <option value="bob.floor">Bob Floor (demo)</option>
                  </select>
                </div>
                {/* PIN dots */}
                <div className="flex justify-center gap-3 my-4">
                  {[0,1,2,3,4,5].map(i => (
                    <div key={i} className={`pin-dot ${i < pin.length ? 'filled' : ''}`}></div>
                  ))}
                </div>
                {/* Numpad */}
                <div className="grid grid-cols-3 gap-3 justify-items-center mb-4">
                  {[1,2,3,4,5,6,7,8,9].map(n => <PinKey key={n} val={String(n)} />)}
                  <PinKey val="del" label="⌫" />
                  <PinKey val="0" />
                  <button className="pin-key no-select bg-[#07324C] text-white border-[#07324C]" onClick={handlePinLogin} disabled={loading}>
                    {loading ? '...' : '✓'}
                  </button>
                </div>
                <p className="text-xs text-gray-400 text-center">Demo PIN: 123456</p>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

// ═══════════════════════════════════════════════════════════════
// SIDEBAR
// ═══════════════════════════════════════════════════════════════
const NAV_ITEMS = [
  { id: 'dashboard', label: 'Dashboard', icon: 'LayoutDashboard', roles: ['executive','admin','office','planner','qa','dispatch','ops_manager'] },
  { id: 'office', label: 'Office', icon: 'Briefcase', roles: ['executive','admin','office','ops_manager'] },
  { id: 'planning', label: 'Planning', icon: 'CalendarDays', roles: ['executive','admin','planner','ops_manager','production_manager'] },
  { id: 'allocation', label: 'Station Alloc', icon: 'Users', roles: ['production_manager','executive','admin','ops_manager','planner'] },
  { id: 'floor', label: 'Production', icon: 'Factory', roles: ['floor_worker','team_leader','production_manager','executive','admin','ops_manager'] },
  { id: 'qa', label: 'QA', icon: 'ShieldCheck', roles: ['qa','team_leader','executive','admin','ops_manager'] },
  { id: 'dispatch', label: 'Dispatch', icon: 'Truck', roles: ['dispatch','executive','admin','ops_manager'] },
  { id: 'ops', label: 'Ops Manager', icon: 'BarChart3', roles: ['executive','admin','ops_manager'] },
  { id: 'admin', label: 'Admin', icon: 'Settings', roles: ['executive','admin'] },
];

const Sidebar = ({ page, onNav, user, onLogout, collapsed }) => {
  const visibleItems = NAV_ITEMS.filter(item => item.roles.includes(user?.role) || user?.role === 'executive' || user?.role === 'admin');

  return (
    <div id="sidebar" className={collapsed ? 'collapsed' : ''} style={{display:'flex',flexDirection:'column',height:'100vh',position:'sticky',top:0}}>
      {/* Logo */}
      <div className="logo-area" style={{display:'flex',alignItems:'center',gap:'10px',padding:'20px 16px',borderBottom:'1px solid rgba(171,191,200,0.2)',flexShrink:0}}>
        <div className="logo-chevron"></div>
        <div className="logo-text" style={{color:'white'}}>
          <div style={{fontWeight:900,fontSize:'14px',letterSpacing:'0.12em'}}>HYNE PALLETS</div>
          <div style={{fontSize:'9px',color:'#ABBFC8',letterSpacing:'0.08em'}}>MFG MANAGEMENT</div>
        </div>
      </div>

      {/* Nav */}
      <nav style={{flex:1,overflowY:'auto',padding:'8px 0'}}>
        {visibleItems.map(item => (
          <div key={item.id} className={`nav-item ${page===item.id?'active':''}`} onClick={() => onNav(item.id)}>
            <Icon name={item.icon} size={18} />
            <span className="nav-label" style={{fontSize:'13px',fontWeight:600}}>{item.label}</span>
          </div>
        ))}
      </nav>

      {/* User info */}
      <div style={{borderTop:'1px solid rgba(171,191,200,0.2)',padding:'12px 8px',flexShrink:0}}>
        <div className="user-info" style={{padding:'8px 12px',marginBottom:'4px'}}>
          <div style={{color:'#ABBFC8',fontSize:'11px',textTransform:'uppercase',letterSpacing:'0.05em',fontWeight:600}}>{user?.role?.replace('_',' ')}</div>
          <div style={{color:'white',fontSize:'13px',fontWeight:600,marginTop:'2px'}}>{user?.full_name}</div>
        </div>
        <div className="nav-item" onClick={onLogout}>
          <Icon name="LogOut" size={18} />
          <span className="nav-label" style={{fontSize:'13px',fontWeight:600}}>Logout</span>
        </div>
      </div>
    </div>
  );
};

// ═══════════════════════════════════════════════════════════════
// DASHBOARD / OPS MANAGER
// ═══════════════════════════════════════════════════════════════
const Dashboard = ({ token, isOps = false }) => {
  const [orderStats, setOrderStats] = useState(null);
  const [prodStats, setProdStats] = useState(null);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    try {
      const [os, ps] = await Promise.all([
        api('/stats/orders', {}, token),
        api('/stats/production', {}, token),
      ]);
      setOrderStats(os);
      setProdStats(ps);
    } catch (e) { toast(e.message, 'error'); }
    setLoading(false);
  }, [token]);

  useEffect(() => { load(); }, [load]);

  if (loading) return <div className="flex items-center justify-center h-64"><Spinner /></div>;

  const pipeline = orderStats?.by_status || [];
  const zones = prodStats?.zone_stats || [];
  const STATUS_ORDER = ['T','C','R','P','F','dispatched','delivered'];
  const pipelineMap = {};
  pipeline.forEach(s => pipelineMap[s.status] = s);

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-black text-[#07324C]">{isOps ? 'Operations Overview' : 'Dashboard'}</h1>
          <p className="text-sm text-gray-500 mt-0.5">{new Date().toLocaleDateString('en-AU',{weekday:'long',day:'numeric',month:'long',year:'numeric'})}</p>
        </div>
        <button className="btn btn-outline btn-sm" onClick={load}><Icon name="RefreshCw" size={14}/> Refresh</button>
      </div>

      {/* Order pipeline */}
      <div className="hp-card">
        <h2 className="text-sm font-bold text-gray-500 uppercase tracking-wider mb-4">Order Pipeline</h2>
        <div className="flex gap-2 overflow-x-auto pb-2">
          {STATUS_ORDER.map(s => {
            const d = pipelineMap[s];
            const colors = { T:'#6b7280', C:'#1d4ed8', R:'#92400e', P:'#9a3412', F:'#166534', dispatched:'#5b21b6', delivered:'#155e75' };
            const bgs = { T:'#f3f4f6', C:'#dbeafe', R:'#fef3c7', P:'#fed7aa', F:'#dcfce7', dispatched:'#ede9fe', delivered:'#cffafe' };
            return (
              <div key={s} className="funnel-step" style={{minWidth:100}}>
                <div style={{background:bgs[s]||'#f3f4f6',borderRadius:8,padding:'12px 16px',textAlign:'center',width:'100%'}}>
                  <div style={{fontSize:28,fontWeight:900,color:colors[s]||'#374151'}}>{d?.count||0}</div>
                  <div style={{fontSize:11,fontWeight:700,color:colors[s]||'#374151',marginTop:2}}>{STATUS_LABELS[s]}</div>
                  {d?.total_value > 0 && <div style={{fontSize:10,color:'#6b7280',marginTop:2}}>${Number(d.total_value).toLocaleString()}</div>}
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Zone production cards */}
      <div>
        <h2 className="text-sm font-bold text-gray-500 uppercase tracking-wider mb-3">Today's Production by Zone</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {zones.map(z => (
            <div key={z.code} className="stat-card" style={{borderLeftColor: z.sessions_today > 0 ? '#22C55E' : '#ABBFC8'}}>
              <div className="text-xs font-bold text-gray-400 uppercase tracking-wider">{z.zone_name}</div>
              <div className="text-3xl font-black text-[#07324C] mt-1">{z.units_produced.toLocaleString()}</div>
              <div className="text-xs text-gray-500 mt-1">{z.sessions_today} session{z.sessions_today !== 1 ? 's' : ''} today</div>
            </div>
          ))}
        </div>
      </div>

      {/* Summary row */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="stat-card" style={{borderLeftColor:'#F59E0B'}}>
          <div className="text-xs font-bold text-gray-400 uppercase tracking-wider">Total Orders</div>
          <div className="text-3xl font-black text-[#07324C] mt-1">{orderStats?.totals?.orders || 0}</div>
          <div className="text-xs text-gray-500 mt-1">All time</div>
        </div>
        <div className="stat-card" style={{borderLeftColor:'#ED1C24'}}>
          <div className="text-xs font-bold text-gray-400 uppercase tracking-wider">Active Sessions</div>
          <div className="text-3xl font-black text-[#07324C] mt-1">{prodStats?.active_sessions || 0}</div>
          <div className="text-xs text-gray-500 mt-1">On floor now</div>
        </div>
        <div className="stat-card" style={{borderLeftColor:'#22C55E'}}>
          <div className="text-xs font-bold text-gray-400 uppercase tracking-wider">Today's Value</div>
          <div className="text-3xl font-black text-[#07324C] mt-1">${Number(prodStats?.today_completed_value || 0).toLocaleString()}</div>
          <div className="text-xs text-gray-500 mt-1">Completed production</div>
        </div>
      </div>

      {/* Alerts */}
      {isOps && (
        <div className="hp-card">
          <h2 className="text-sm font-bold text-gray-500 uppercase tracking-wider mb-3">Alerts</h2>
          {(pipelineMap['T']?.count > 0) && (
            <div className="flex items-center gap-3 p-3 bg-amber-50 border border-amber-200 rounded-lg mb-2">
              <Icon name="AlertTriangle" size={16} className="text-amber-600" />
              <span className="text-sm font-medium text-amber-800">{pipelineMap['T']?.count} orders awaiting office verification</span>
            </div>
          )}
          {(pipelineMap['F']?.count > 0) && (
            <div className="flex items-center gap-3 p-3 bg-blue-50 border border-blue-200 rounded-lg mb-2">
              <Icon name="Package" size={16} className="text-blue-600" />
              <span className="text-sm font-medium text-blue-800">{pipelineMap['F']?.count} finished orders awaiting dispatch</span>
            </div>
          )}
          {!pipelineMap['T']?.count && !pipelineMap['F']?.count && (
            <p className="text-sm text-gray-400">No active alerts.</p>
          )}
        </div>
      )}
    </div>
  );
};

// ═══════════════════════════════════════════════════════════════
// OFFICE DASHBOARD
// ═══════════════════════════════════════════════════════════════
const OfficeDashboard = ({ token }) => {
  const [tab, setTab] = useState('unverified');
  const [orders, setOrders] = useState([]);
  const [allOrders, setAllOrders] = useState([]);
  const [loading, setLoading] = useState(true);
  const [expanded, setExpanded] = useState(null);
  const [historySearch, setHistorySearch] = useState('');
  const [historyStatus, setHistoryStatus] = useState('');
  const [verifyData, setVerifyData] = useState({});

  // New Client form state
  const [clients, setClients] = useState([]);
  const [skus, setSkus] = useState([]);
  const [clientForm, setClientForm] = useState({ company_name:'', contact_name:'', email:'', phone:'', address:'', payment_terms:'30 days' });
  const [clientSaving, setClientSaving] = useState(false);

  // New Order form state
  const [orderForm, setOrderForm] = useState({ order_number:'', client_id:'', delivery_type:'delivery', special_instructions:'', notes:'' });
  const [orderLines, setOrderLines] = useState([{ sku_id:'', quantity:1, special_instructions:'', skuSearch:'' }]);
  const [orderSaving, setOrderSaving] = useState(false);

  const loadOrders = useCallback(async () => {
    setLoading(true);
    try {
      const [unver, all] = await Promise.all([
        api('/orders?status=T', {}, token),
        api('/orders', {}, token),
      ]);
      setOrders(unver.filter(o => !o.is_verified));
      setAllOrders(all);
    } catch (e) { toast(e.message, 'error'); }
    setLoading(false);
  }, [token]);

  useEffect(() => { loadOrders(); }, [loadOrders]);

  // Load clients and SKUs for forms
  const loadRefData = useCallback(async () => {
    try {
      const [c, s] = await Promise.all([
        api('/clients', {}, token),
        api('/skus', {}, token),
      ]);
      setClients(c); setSkus(s);
    } catch (e) { console.error(e); }
  }, [token]);
  useEffect(() => { loadRefData(); }, [loadRefData]);

  // Generate next order number
  const generateOrderNumber = () => {
    const d = new Date();
    const prefix = 'ORD-' + d.getFullYear() + String(d.getMonth()+1).padStart(2,'0');
    const seq = String(Math.floor(Math.random()*9000)+1000);
    return prefix + '-' + seq;
  };

  // Submit new client
  const submitClient = async () => {
    if (!clientForm.company_name.trim()) { toast('Company name is required', 'warning'); return; }
    setClientSaving(true);
    try {
      await api('/clients', { method:'POST', body: JSON.stringify(clientForm) }, token);
      toast('Client created successfully', 'success');
      setClientForm({ company_name:'', contact_name:'', email:'', phone:'', address:'', payment_terms:'30 days' });
      loadRefData();
    } catch (e) { toast(e.message, 'error'); }
    setClientSaving(false);
  };

  // Submit new order
  const submitOrder = async () => {
    if (!orderForm.order_number.trim()) { toast('Order number is required', 'warning'); return; }
    if (!orderForm.client_id) { toast('Please select a client', 'warning'); return; }
    const validLines = orderLines.filter(l => l.sku_id && l.quantity > 0);
    if (validLines.length === 0) { toast('Add at least one line item with a SKU and quantity', 'warning'); return; }
    setOrderSaving(true);
    try {
      const order = await api('/orders', { method:'POST', body: JSON.stringify(orderForm) }, token);
      // Add line items
      for (const line of validLines) {
        const sku = skus.find(s => s.id === parseInt(line.sku_id));
        await api(`/orders/${order.id}/items`, { method:'POST', body: JSON.stringify({
          sku_id: parseInt(line.sku_id),
          sku_code: sku?.code || '',
          product_name: sku?.name || '',
          quantity: parseInt(line.quantity),
          zone_id: sku?.zone_id || null,
          special_instructions: line.special_instructions || null
        })}, token);
      }
      toast(`Order ${order.order_number} created with ${validLines.length} line item(s)`, 'success');
      setOrderForm({ order_number:'', client_id:'', delivery_type:'delivery', special_instructions:'', notes:'' });
      setOrderLines([{ sku_id:'', quantity:1, special_instructions:'', skuSearch:'' }]);
      loadOrders();
      setTab('unverified');
    } catch (e) { toast(e.message, 'error'); }
    setOrderSaving(false);
  };

  // Order line helpers
  const addLine = () => setOrderLines(prev => [...prev, { sku_id:'', quantity:1, special_instructions:'', skuSearch:'' }]);
  const removeLine = (idx) => setOrderLines(prev => prev.length <= 1 ? prev : prev.filter((_,i) => i !== idx));
  const updateLine = (idx, field, val) => setOrderLines(prev => prev.map((l,i) => i === idx ? { ...l, [field]: val } : l));

  const verifyOrder = async (orderId) => {
    try {
      await api(`/orders/${orderId}/verify`, { method: 'PUT' }, token);
      toast('Order verified successfully', 'success');
      loadOrders();
      setExpanded(null);
    } catch (e) { toast(e.message, 'error'); }
  };

  const setETA = async (orderId, etaDate) => {
    if (!etaDate) { toast('Please select a date', 'warning'); return; }
    try {
      await api(`/orders/${orderId}/eta`, { method: 'PUT', body: JSON.stringify({ eta_date: etaDate }) }, token);
      toast('ETA set successfully', 'success');
      loadOrders();
    } catch (e) { toast(e.message, 'error'); }
  };

  const scheduledOrders = allOrders.filter(o => ['C','R'].includes(o.status) && !o.eta_date);
  const historyFiltered = allOrders.filter(o => {
    const matchSearch = !historySearch || o.order_number?.toLowerCase().includes(historySearch.toLowerCase()) || o.client_name?.toLowerCase().includes(historySearch.toLowerCase());
    const matchStatus = !historyStatus || o.status === historyStatus;
    return matchSearch && matchStatus;
  });

  return (
    <div className="p-6 space-y-4">
      <h1 className="text-2xl font-black text-[#07324C]">Office Dashboard</h1>

      <div className="tab-bar">
        <div className={`tab-item ${tab==='neworder'?'active':''}`} onClick={()=>{ setTab('neworder'); if(!orderForm.order_number) setOrderForm(f=>({...f, order_number: generateOrderNumber()})); }}>
          <Icon name="FilePlus" size={14} /> New Order
        </div>
        <div className={`tab-item ${tab==='newclient'?'active':''}`} onClick={()=>setTab('newclient')}>
          <Icon name="UserPlus" size={14} /> New Client
        </div>
        <div className={`tab-item ${tab==='unverified'?'active':''}`} onClick={()=>setTab('unverified')}>
          Pending {orders.length > 0 && <span className="ml-1.5 bg-red-500 text-white text-xs rounded-full px-2">{orders.length}</span>}
        </div>
        <div className={`tab-item ${tab==='eta'?'active':''}`} onClick={()=>setTab('eta')}>
          Set ETA {scheduledOrders.length > 0 && <span className="ml-1.5 bg-amber-500 text-white text-xs rounded-full px-2">{scheduledOrders.length}</span>}
        </div>
        <div className={`tab-item ${tab==='history'?'active':''}`} onClick={()=>setTab('history')}>Order History</div>
      </div>

      {loading ? <div className="flex justify-center py-12"><Spinner /></div> : (
        <>
          {/* NEW ORDER */}
          {tab === 'neworder' && (
            <div className="hp-card">
              <h2 className="text-sm font-bold text-gray-500 uppercase tracking-wider mb-4">Create New Order</h2>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
                <div>
                  <label className="block text-xs font-bold text-gray-600 mb-1">Order Number *</label>
                  <div className="flex gap-2">
                    <input className="hp-input flex-1" value={orderForm.order_number} onChange={e => setOrderForm(f=>({...f, order_number: e.target.value}))} placeholder="ORD-202602-0001" />
                    <button className="btn btn-sm" style={{background:'#e2e8f0',color:'#07324C'}} onClick={() => setOrderForm(f=>({...f, order_number: generateOrderNumber()}))}>Generate</button>
                  </div>
                </div>
                <div>
                  <label className="block text-xs font-bold text-gray-600 mb-1">Client *</label>
                  <select className="hp-input" value={orderForm.client_id} onChange={e => setOrderForm(f=>({...f, client_id: e.target.value}))}>
                    <option value="">— Select Client —</option>
                    {clients.map(c => <option key={c.id} value={c.id}>{c.company_name}</option>)}
                  </select>
                </div>
                <div>
                  <label className="block text-xs font-bold text-gray-600 mb-1">Delivery Type</label>
                  <select className="hp-input" value={orderForm.delivery_type} onChange={e => setOrderForm(f=>({...f, delivery_type: e.target.value}))}>
                    <option value="delivery">Delivery</option>
                    <option value="collection">Collection</option>
                  </select>
                </div>
                <div>
                  <label className="block text-xs font-bold text-gray-600 mb-1">Special Instructions</label>
                  <input className="hp-input" value={orderForm.special_instructions} onChange={e => setOrderForm(f=>({...f, special_instructions: e.target.value}))} placeholder="e.g. Heat treated, specific stencil..." />
                </div>
                <div className="md:col-span-2">
                  <label className="block text-xs font-bold text-gray-600 mb-1">Notes</label>
                  <textarea className="hp-input" rows="2" value={orderForm.notes} onChange={e => setOrderForm(f=>({...f, notes: e.target.value}))} placeholder="Internal notes..." />
                </div>
              </div>

              <h3 className="text-sm font-bold text-gray-500 uppercase tracking-wider mb-3">Line Items</h3>
              <div className="space-y-3 mb-4">
                {orderLines.map((line, idx) => {
                  const selectedSku = skus.find(s => s.id === parseInt(line.sku_id));
                  return (
                    <div key={idx} className="flex flex-wrap items-end gap-3 p-3 bg-gray-50 rounded-lg border">
                      <div className="flex-1" style={{minWidth:260}}>
                        <label className="block text-xs font-bold text-gray-600 mb-1">Product / SKU *</label>
                        <input className="hp-input mb-1" placeholder="Type to search SKUs..." value={line.skuSearch} onChange={e => { updateLine(idx, 'skuSearch', e.target.value); if(line.sku_id) updateLine(idx, 'sku_id', ''); }} />
                        <select className="hp-input" value={line.sku_id} onChange={e => { updateLine(idx, 'sku_id', e.target.value); const s = skus.find(sk=>sk.id===parseInt(e.target.value)); if(s) updateLine(idx, 'skuSearch', s.code + ' ' + s.name); }}>
                          <option value="">— {line.skuSearch ? `${skus.filter(s => { const q = line.skuSearch.toLowerCase(); return s.code.toLowerCase().includes(q) || s.name.toLowerCase().includes(q); }).length} matches` : 'Select SKU'} —</option>
                          {skus.filter(s => { if (!line.skuSearch) return true; const q = line.skuSearch.toLowerCase(); return s.code.toLowerCase().includes(q) || s.name.toLowerCase().includes(q); }).map(s => <option key={s.id} value={s.id}>{s.code} — {s.name}</option>)}
                        </select>
                        {selectedSku && <span className="text-xs text-gray-400 mt-0.5 block">Zone: {selectedSku.zone_id === 1 ? 'Viking' : selectedSku.zone_id === 2 ? 'Handmade' : selectedSku.zone_id === 3 ? 'DTL' : 'Crates'}</span>}
                      </div>
                      <div style={{width:100}}>
                        <label className="block text-xs font-bold text-gray-600 mb-1">Qty *</label>
                        <input type="number" min="1" className="hp-input" value={line.quantity} onChange={e => updateLine(idx, 'quantity', e.target.value)} />
                      </div>
                      <div className="flex-1" style={{minWidth:180}}>
                        <label className="block text-xs font-bold text-gray-600 mb-1">Line Notes</label>
                        <input className="hp-input" value={line.special_instructions} onChange={e => updateLine(idx, 'special_instructions', e.target.value)} placeholder="Optional" />
                      </div>
                      <button className="btn btn-sm" style={{background:'#fee2e2',color:'#991b1b'}} onClick={() => removeLine(idx)} disabled={orderLines.length <= 1}>
                        <Icon name="Trash2" size={14} />
                      </button>
                    </div>
                  );
                })}
              </div>
              <div className="flex items-center gap-3">
                <button className="btn btn-sm" style={{background:'#e2e8f0',color:'#07324C'}} onClick={addLine}>
                  <Icon name="Plus" size={14} /> Add Line Item
                </button>
                <div className="flex-1" />
                <button className="btn btn-primary" onClick={submitOrder} disabled={orderSaving}>
                  {orderSaving ? 'Creating...' : 'Create Order'}
                </button>
              </div>
            </div>
          )}

          {/* NEW CLIENT */}
          {tab === 'newclient' && (
            <div className="hp-card">
              <h2 className="text-sm font-bold text-gray-500 uppercase tracking-wider mb-4">Add New Client</h2>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
                <div>
                  <label className="block text-xs font-bold text-gray-600 mb-1">Company Name *</label>
                  <input className="hp-input" value={clientForm.company_name} onChange={e => setClientForm(f=>({...f, company_name: e.target.value}))} placeholder="e.g. ABC Logistics Pty Ltd" />
                </div>
                <div>
                  <label className="block text-xs font-bold text-gray-600 mb-1">Contact Name</label>
                  <input className="hp-input" value={clientForm.contact_name} onChange={e => setClientForm(f=>({...f, contact_name: e.target.value}))} placeholder="e.g. John Smith" />
                </div>
                <div>
                  <label className="block text-xs font-bold text-gray-600 mb-1">Email</label>
                  <input type="email" className="hp-input" value={clientForm.email} onChange={e => setClientForm(f=>({...f, email: e.target.value}))} placeholder="john@abclogistics.com.au" />
                </div>
                <div>
                  <label className="block text-xs font-bold text-gray-600 mb-1">Phone</label>
                  <input className="hp-input" value={clientForm.phone} onChange={e => setClientForm(f=>({...f, phone: e.target.value}))} placeholder="07 3000 0000" />
                </div>
                <div className="md:col-span-2">
                  <label className="block text-xs font-bold text-gray-600 mb-1">Address</label>
                  <input className="hp-input" value={clientForm.address} onChange={e => setClientForm(f=>({...f, address: e.target.value}))} placeholder="Full address" />
                </div>
                <div>
                  <label className="block text-xs font-bold text-gray-600 mb-1">Payment Terms</label>
                  <select className="hp-input" value={clientForm.payment_terms} onChange={e => setClientForm(f=>({...f, payment_terms: e.target.value}))}>
                    <option value="7 days">7 Days</option>
                    <option value="14 days">14 Days</option>
                    <option value="30 days">30 Days</option>
                    <option value="60 days">60 Days</option>
                    <option value="COD">COD</option>
                    <option value="Prepaid">Prepaid</option>
                  </select>
                </div>
              </div>
              <div className="flex items-center gap-3">
                <button className="btn btn-primary" onClick={submitClient} disabled={clientSaving}>
                  {clientSaving ? 'Saving...' : 'Add Client'}
                </button>
                <span className="text-xs text-gray-400">Client will be available for new orders immediately</span>
              </div>

              {/* Existing clients quick-reference */}
              {clients.length > 0 && (
                <div className="mt-6 pt-4 border-t">
                  <h3 className="text-xs font-bold text-gray-400 uppercase tracking-wider mb-3">Existing Clients ({clients.length})</h3>
                  <div className="table-scroll">
                    <table className="hp-table">
                      <thead>
                        <tr><th>Company</th><th>Contact</th><th>Email</th><th>Phone</th><th>Terms</th></tr>
                      </thead>
                      <tbody>
                        {clients.map(c => (
                          <tr key={c.id}>
                            <td className="font-bold text-[#07324C]">{c.company_name}</td>
                            <td>{c.contact_name || '—'}</td>
                            <td className="text-xs">{c.email || '—'}</td>
                            <td className="text-xs">{c.phone || '—'}</td>
                            <td className="text-xs">{c.payment_terms || '—'}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* UNVERIFIED */}
          {tab === 'unverified' && (
            <div className="hp-card">
              <h2 className="text-sm font-bold text-gray-500 uppercase tracking-wider mb-4">Pending Verification ({orders.length})</h2>
              {orders.length === 0 ? <Empty msg="All orders are verified — great work!" /> : (
                <div className="table-scroll">
                  <table className="hp-table">
                    <thead>
                      <tr><th>Order #</th><th>Client</th><th>Items</th><th>Date</th><th>Special Instructions</th><th></th></tr>
                    </thead>
                    <tbody>
                      {orders.map(o => (
                        <React.Fragment key={o.id}>
                          <tr>
                            <td className="font-bold text-[#07324C]">{o.order_number}</td>
                            <td>{o.client_name}</td>
                            <td>{o.item_count} item{o.item_count!==1?'s':''} · {o.total_qty} units</td>
                            <td>{o.created_at?.split('T')[0]}</td>
                            <td className="text-xs text-gray-500 max-w-48 truncate">{o.special_instructions || '—'}</td>
                            <td>
                              <button className="btn btn-primary btn-sm" onClick={() => setExpanded(expanded===o.id?null:o.id)}>
                                {expanded===o.id ? 'Close' : 'Review'}
                              </button>
                            </td>
                          </tr>
                          {expanded === o.id && (
                            <tr>
                              <td colSpan="6" className="p-0">
                                <OrderVerifyPanel order={o} token={token} onVerify={()=>verifyOrder(o.id)} />
                              </td>
                            </tr>
                          )}
                        </React.Fragment>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}

          {/* SET ETA */}
          {tab === 'eta' && (
            <div className="hp-card">
              <div className="flex items-start justify-between mb-4">
                <div>
                  <h2 className="text-sm font-bold text-gray-500 uppercase tracking-wider">Orders Needing Dispatch ETA ({scheduledOrders.length})</h2>
                  <p className="text-xs text-gray-400 mt-1">Set the expected dispatch/delivery date for transport scheduling. Use the scheduled production date as a guide and add buffer days as needed.</p>
                </div>
              </div>
              {scheduledOrders.length === 0 ? <Empty msg="All scheduled orders have ETAs set" /> : (
                <div className="table-scroll">
                  <table className="hp-table">
                    <thead>
                      <tr>
                        <th>Order #</th>
                        <th>Client</th>
                        <th>Status</th>
                        <th>Total Qty</th>
                        <th>Scheduled Prod. Date</th>
                        <th>Dispatch ETA</th>
                      </tr>
                    </thead>
                    <tbody>
                      {scheduledOrders.map(o => {
                        const key = `eta_${o.id}`;
                        // Default to mfg_completion_date + 1 day if available
                        const defaultEta = (() => {
                          if (!verifyData[key] && o.mfg_completion_date) {
                            const d = new Date(o.mfg_completion_date + 'T00:00:00');
                            d.setDate(d.getDate() + 1);
                            return d.toISOString().split('T')[0];
                          }
                          return verifyData[key] || '';
                        })();
                        const etaVal = verifyData[key] !== undefined ? verifyData[key] : defaultEta;
                        return (
                          <tr key={o.id}>
                            <td className="font-bold text-[#07324C]">{o.order_number}</td>
                            <td>{o.client_name}</td>
                            <td><StatusBadge status={o.status} /></td>
                            <td>{o.total_qty}</td>
                            <td>
                              {o.mfg_completion_date ? (
                                <span className="text-blue-700 font-semibold text-xs">
                                  {new Date(o.mfg_completion_date + 'T00:00:00').toLocaleDateString('en-AU',{day:'numeric',month:'short',year:'numeric'})}
                                </span>
                              ) : (
                                <span className="text-gray-400 text-xs italic">Not scheduled</span>
                              )}
                            </td>
                            <td>
                              <div className="flex items-center gap-2">
                                <input type="date" className="hp-input" style={{width:160}}
                                  value={etaVal}
                                  onChange={e=>setVerifyData(p=>({...p,[key]:e.target.value}))}
                                />
                                <button className="btn btn-primary btn-sm" onClick={()=>setETA(o.id, etaVal)}>Confirm</button>
                              </div>
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}

          {/* HISTORY */}
          {tab === 'history' && (
            <div className="hp-card">
              <div className="flex flex-wrap gap-3 mb-4">
                <input className="hp-input" style={{maxWidth:250}} placeholder="Search order # or client…" value={historySearch} onChange={e=>setHistorySearch(e.target.value)} />
                <select className="hp-input" style={{maxWidth:160}} value={historyStatus} onChange={e=>setHistoryStatus(e.target.value)}>
                  <option value="">All Statuses</option>
                  {['T','C','R','P','F','dispatched','delivered','collected'].map(s => <option key={s} value={s}>{STATUS_LABELS[s]}</option>)}
                </select>
              </div>
              <div className="table-scroll">
                <table className="hp-table">
                  <thead>
                    <tr><th>Order #</th><th>Client</th><th>Status</th><th>Items</th><th>Total Qty</th><th>ETA</th><th>Verified</th><th>Created</th></tr>
                  </thead>
                  <tbody>
                    {historyFiltered.slice(0,100).map(o => (
                      <tr key={o.id}>
                        <td className="font-bold text-[#07324C]">{o.order_number}</td>
                        <td>{o.client_name}</td>
                        <td><StatusBadge status={o.status} /></td>
                        <td>{o.item_count}</td>
                        <td>{o.total_qty}</td>
                        <td>{o.eta_date||'—'}</td>
                        <td>{o.is_verified ? <span className="text-green-600 font-bold text-xs">✓ Yes</span> : <span className="text-gray-400 text-xs">No</span>}</td>
                        <td className="text-xs text-gray-500">{o.created_at?.split('T')[0]}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {historyFiltered.length === 0 && <Empty />}
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
};

// Order verify panel (inline expansion)
const OrderVerifyPanel = ({ order, token, onVerify }) => {
  const [items, setItems] = useState([]);
  const [checked1, setChecked1] = useState(false);
  const [checked2, setChecked2] = useState(false);

  useEffect(() => {
    api(`/orders/${order.id}/items`, {}, token).then(setItems).catch(()=>{});
  }, [order.id]);

  return (
    <div className="p-6 bg-blue-50 border-t border-blue-100">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div>
          <h3 className="font-bold text-[#07324C] mb-3">Order Details — {order.order_number}</h3>
          <div className="space-y-2 text-sm">
            <div><span className="text-gray-500">Client:</span> <span className="font-medium">{order.client_name}</span></div>
            <div><span className="text-gray-500">Delivery Type:</span> <span className="font-medium capitalize">{order.delivery_type||'—'}</span></div>
            {order.special_instructions && (
              <div className="p-3 bg-amber-50 border border-amber-200 rounded-lg">
                <span className="font-bold text-amber-800">Special Instructions:</span>
                <p className="text-amber-700 mt-1">{order.special_instructions}</p>
              </div>
            )}
          </div>
          <div className="mt-4">
            <h4 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-2">Line Items</h4>
            <table className="hp-table text-xs">
              <thead><tr><th>SKU</th><th>Product</th><th>Qty</th><th>Zone</th></tr></thead>
              <tbody>
                {items.map(i => (
                  <tr key={i.id}>
                    <td className="font-mono">{i.sku_code||'—'}</td>
                    <td>{i.product_name||'—'}</td>
                    <td>{i.quantity}</td>
                    <td>{i.zone_name||'—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
        <div>
          <h3 className="font-bold text-[#07324C] mb-3">Verification Checklist</h3>
          <div className="space-y-3 mb-6">
            <label className="flex items-start gap-3 cursor-pointer">
              <input type="checkbox" className="mt-0.5 w-4 h-4" checked={checked1} onChange={e=>setChecked1(e.target.checked)} />
              <span className="text-sm">All order fields verified — quantities, SKUs, client details match the original order</span>
            </label>
            <label className="flex items-start gap-3 cursor-pointer">
              <input type="checkbox" className="mt-0.5 w-4 h-4" checked={checked2} onChange={e=>setChecked2(e.target.checked)} />
              <span className="text-sm">Special instructions reviewed and captured in the system</span>
            </label>
          </div>
          <button className="btn btn-success btn-lg w-full justify-center" disabled={!checked1||!checked2} onClick={onVerify}>
            <Icon name="CheckCircle" size={18} /> Confirm & Send Acknowledgement
          </button>
        </div>
      </div>
    </div>
  );
};


// ═══════════════════════════════════════════════════════════════
// PLANNING BOARD — Viking & Handmade weekly grid
// ═══════════════════════════════════════════════════════════════

// ---- Stock Run Modal ----
const StockRunModal = ({ token, zones, onClose, onCreated }) => {
  const [sku, setSku] = useState('');
  const [skuList, setSkuList] = useState([]);
  const [skuSearch, setSkuSearch] = useState('');
  const [selectedSku, setSelectedSku] = useState(null);
  const [quantity, setQuantity] = useState('');
  const [zoneId, setZoneId] = useState('');
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    api('/skus', {}, token).then(setSkuList).catch(()=>{});
  }, []);

  const submit = async () => {
    if (!sku || !quantity) { toast('Select SKU and quantity','warning'); return; }
    setLoading(true);
    try {
      const orderNum = 'STOCK-' + Date.now().toString().slice(-6);
      const skuObj = skuList.find(s => s.id === parseInt(sku));
      const order = await api('/orders', {
        method: 'POST',
        body: JSON.stringify({
          order_number: orderNum,
          client_id: null,
          is_stock_run: 1,
          status: 'C',
          is_verified: 1,
          notes: `Stock run: ${skuObj?.name || sku}`
        })
      }, token);
      // Add order item
      await api(`/orders/${order.id}/items`, {
        method: 'POST',
        body: JSON.stringify({
          sku_id: parseInt(sku),
          quantity: parseInt(quantity),
          zone_id: parseInt(zoneId) || skuObj?.zone_id
        })
      }, token);
      toast('Stock run created', 'success');
      onCreated();
      onClose();
    } catch(e) { toast(e.message, 'error'); }
    setLoading(false);
  };

  // Search filter — all zones, match on code or name
  const filteredSkus = skuSearch.length > 0
    ? skuList.filter(s => {
        const q = skuSearch.toLowerCase();
        return s.code.toLowerCase().includes(q) || s.name.toLowerCase().includes(q);
      })
    : skuList;

  return (
    <div style={{position:'fixed',inset:0,background:'rgba(0,0,0,0.5)',zIndex:1000,display:'flex',alignItems:'center',justifyContent:'center'}}>
      <div style={{background:'white',borderRadius:12,padding:24,width:420,boxShadow:'0 20px 60px rgba(0,0,0,0.3)'}}>
        <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:16}}>
          <h2 style={{fontWeight:900,color:'#07324C',fontSize:18,margin:0}}>Create Stock Run</h2>
          <button onClick={onClose} style={{background:'none',border:'none',cursor:'pointer',fontSize:20,color:'#9ca3af'}}>&times;</button>
        </div>
        <div style={{marginBottom:12,position:'relative'}}>
          <label style={{fontSize:12,fontWeight:700,color:'#6b7280',display:'block',marginBottom:4}}>SKU</label>
          <input
            type="text"
            value={skuSearch}
            onChange={e => { setSkuSearch(e.target.value); setSelectedSku(null); setSku(''); }}
            className="hp-input"
            placeholder="Search SKU code or name..."
            autoComplete="off"
          />
          {skuSearch && !selectedSku && filteredSkus.length > 0 && (
            <div style={{position:'absolute',top:'100%',left:0,right:0,background:'white',border:'1px solid #e5e7eb',borderRadius:8,maxHeight:220,overflowY:'auto',zIndex:10,boxShadow:'0 4px 12px rgba(0,0,0,0.1)',marginTop:2}}>
              {filteredSkus.slice(0,30).map(s => {
                const zoneName = zones.find(z=>z.id===s.zone_id)?.name || '';
                return (
                  <div key={s.id} onClick={() => {
                    setSelectedSku(s);
                    setSku(String(s.id));
                    setSkuSearch(`${s.code} — ${s.name}`);
                    setZoneId(String(s.zone_id));
                  }}
                  onMouseEnter={e => e.currentTarget.style.background='#f0f4ff'}
                  onMouseLeave={e => e.currentTarget.style.background='white'}
                  style={{padding:'8px 12px',cursor:'pointer',fontSize:12,borderBottom:'1px solid #f3f4f6',display:'flex',justifyContent:'space-between',alignItems:'center'}}>
                    <span><strong>{s.code}</strong> — {s.name}</span>
                    <span style={{fontSize:10,color:'#9ca3af',marginLeft:8,whiteSpace:'nowrap'}}>{zoneName}</span>
                  </div>
                );
              })}
              {filteredSkus.length > 30 && <div style={{padding:'6px 12px',fontSize:11,color:'#9ca3af',textAlign:'center'}}>{filteredSkus.length - 30} more results...</div>}
            </div>
          )}
          {skuSearch && !selectedSku && filteredSkus.length === 0 && (
            <div style={{position:'absolute',top:'100%',left:0,right:0,background:'white',border:'1px solid #e5e7eb',borderRadius:8,padding:'12px',textAlign:'center',fontSize:12,color:'#9ca3af',boxShadow:'0 4px 12px rgba(0,0,0,0.1)',marginTop:2}}>No matching SKUs</div>
          )}
          {selectedSku && <div style={{fontSize:11,color:'#6b7280',marginTop:4}}>Zone: {zones.find(z=>z.id===selectedSku.zone_id)?.name || 'Unknown'} &middot; Labour: ${selectedSku.labour_cost?.toFixed(2) || '0.00'} &middot; Sell: ${selectedSku.sell_price?.toFixed(2) || '0.00'}</div>}
        </div>
        <div style={{marginBottom:12}}>
          <label style={{fontSize:12,fontWeight:700,color:'#6b7280',display:'block',marginBottom:4}}>Quantity</label>
          <input type="number" min="1" value={quantity} onChange={e=>setQuantity(e.target.value)} className="hp-input" placeholder="e.g. 500" />
        </div>
        <div style={{display:'flex',gap:8,marginTop:20}}>
          <button className="btn btn-primary flex-1 justify-center" onClick={submit} disabled={loading}>
            {loading ? <Spinner /> : 'Create Stock Run'}
          </button>
          <button className="btn btn-secondary" onClick={onClose}>Cancel</button>
        </div>
      </div>
    </div>
  );
};

// ---- Split Modal ----
const SplitModal = ({ entry, token, onClose, onSplit }) => {
  const [splitQty, setSplitQty] = useState('');
  const [loading, setLoading] = useState(false);
  const maxQty = (entry.planned_quantity || entry.item_quantity || 0) - 1;

  const submit = async () => {
    const qty = parseInt(splitQty);
    if (!qty || qty <= 0 || qty >= (entry.planned_quantity || entry.item_quantity)) {
      toast('Enter a valid split quantity (less than total)', 'warning'); return;
    }
    setLoading(true);
    try {
      if (entry.order_item_id) {
        await api(`/order-items/${entry.order_item_id}/split`, {
          method: 'POST',
          body: JSON.stringify({ new_quantity: qty })
        }, token);
        toast('Work order split', 'success');
        onSplit();
        onClose();
      }
    } catch(e) { toast(e.message, 'error'); }
    setLoading(false);
  };

  return (
    <div style={{position:'fixed',inset:0,background:'rgba(0,0,0,0.5)',zIndex:1000,display:'flex',alignItems:'center',justifyContent:'center'}}>
      <div style={{background:'white',borderRadius:12,padding:24,width:360,boxShadow:'0 20px 60px rgba(0,0,0,0.3)'}}>
        <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:16}}>
          <h2 style={{fontWeight:900,color:'#07324C',fontSize:16,margin:0}}>Split Work Order</h2>
          <button onClick={onClose} style={{background:'none',border:'none',cursor:'pointer',fontSize:20,color:'#9ca3af'}}>×</button>
        </div>
        <p style={{fontSize:12,color:'#6b7280',marginBottom:12}}>
          Split <strong>{entry.order_number}</strong> ({entry.planned_quantity || entry.item_quantity} units)<br/>
          Enter the quantity for the NEW split item. Original will be reduced.
        </p>
        <input type="number" min="1" max={maxQty} value={splitQty} onChange={e=>setSplitQty(e.target.value)} className="hp-input" placeholder={`1 – ${maxQty}`} />
        <div style={{display:'flex',gap:8,marginTop:16}}>
          <button className="btn btn-primary flex-1 justify-center" onClick={submit} disabled={loading}>
            {loading ? <Spinner /> : 'Split'}
          </button>
          <button className="btn btn-secondary" onClick={onClose}>Cancel</button>
        </div>
      </div>
    </div>
  );
};

// ---- Work Order Card (in grid) ----
const WOCard = ({ entry, onRemove, onSplit, onEditQty, onDockingComplete }) => {
  const [showMenu, setShowMenu] = useState(false);
  const [editingQty, setEditingQty] = useState(false);
  const [qtyVal, setQtyVal] = useState(entry.planned_quantity || 0);

  const isStock = entry.is_stock_run;
  const hasPriority = !!entry.requested_delivery_date;
  const qty = entry.planned_quantity || entry.item_quantity || 0;
  const isDocking = entry.order_status === 'C';

  return (
    <div
      style={{
        background: isDocking ? '#eff6ff' : '#dcfce7',
        border: isDocking ? '1.5px solid #2563eb' : '1.5px solid #16a34a',
        borderRadius: 5,
        padding: '4px 6px',
        marginBottom: 3,
        fontSize: 10,
        position: 'relative',
        cursor: 'default'
      }}
      onContextMenu={e => { e.preventDefault(); setShowMenu(true); }}
    >
      {/* Context menu */}
      {showMenu && (
        <div style={{
          position: 'absolute', top: 0, right: 0, background: 'white',
          border: '1px solid #e2e8f0', borderRadius: 6, boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
          zIndex: 50, minWidth: 130, padding: '4px 0', fontSize: 11
        }} onMouseLeave={() => setShowMenu(false)}>
          <button style={{display:'block',width:'100%',padding:'5px 12px',textAlign:'left',background:'none',border:'none',cursor:'pointer',color:'#374151'}} onClick={() => { setEditingQty(true); setShowMenu(false); }}>Edit Quantity</button>
          <button style={{display:'block',width:'100%',padding:'5px 12px',textAlign:'left',background:'none',border:'none',cursor:'pointer',color:'#374151'}} onClick={() => { onSplit(entry); setShowMenu(false); }}>Split Work Order</button>
          {isDocking && onDockingComplete && (
            <>
              <div style={{borderTop:'1px solid #dbeafe',margin:'4px 0'}}></div>
              <button style={{display:'block',width:'100%',padding:'5px 12px',textAlign:'left',background:'none',border:'none',cursor:'pointer',color:'#2563eb',fontWeight:600}} onClick={() => { onDockingComplete(entry); setShowMenu(false); }}>✓ Docking Complete</button>
            </>
          )}
          <div style={{borderTop:'1px solid #f3f4f6',margin:'4px 0'}}></div>
          <button style={{display:'block',width:'100%',padding:'5px 12px',textAlign:'left',background:'none',border:'none',cursor:'pointer',color:'#ef4444'}} onClick={() => { onRemove(entry.id); setShowMenu(false); }}>Remove</button>
        </div>
      )}

      <div style={{display:'flex',alignItems:'flex-start',justifyContent:'space-between',gap:2}}>
        <div style={{flex:1,minWidth:0}}>
          <div style={{display:'flex',alignItems:'center',gap:3,flexWrap:'wrap'}}>
            {isDocking && <span style={{background:'#2563eb',color:'white',fontSize:8,fontWeight:700,padding:'0 4px',borderRadius:3}}>🔶 DOCKING</span>}
            {isStock && <span style={{background:'#f59e0b',color:'white',fontSize:8,fontWeight:700,padding:'0 4px',borderRadius:3}}>STOCK</span>}
            {hasPriority && <span style={{background:'#ef4444',color:'white',fontSize:8,fontWeight:700,padding:'0 4px',borderRadius:3}}>⚡</span>}
            <span style={{fontWeight:700,color: isDocking ? '#1e3a8a' : '#166534',fontSize:10,overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}}>{entry.order_number || '—'}</span>
          </div>
          <div style={{color:'#4b5563',fontSize:9,marginTop:1,overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}}>{entry.sku_code || entry.product_name || '—'}</div>
          {entry.requested_delivery_date && (
            <div style={{color:'#ef4444',fontSize:8,fontWeight:600}}>Due: {entry.requested_delivery_date}</div>
          )}
        </div>
        <div style={{display:'flex',flexDirection:'column',alignItems:'flex-end',gap:2}}>
          {editingQty ? (
            <form onSubmit={e => { e.preventDefault(); onEditQty(entry.id, parseInt(qtyVal)); setEditingQty(false); }} style={{display:'flex',gap:2}}>
              <input type="number" min="1" value={qtyVal} onChange={e=>setQtyVal(e.target.value)}
                style={{width:50,fontSize:9,border:'1px solid #d1d5db',borderRadius:3,padding:'1px 3px'}} autoFocus />
              <button type="submit" style={{background:'#16a34a',color:'white',border:'none',borderRadius:3,fontSize:9,padding:'1px 4px',cursor:'pointer'}}>✓</button>
            </form>
          ) : (
            <span style={{background:'#16a34a',color:'white',fontSize:9,fontWeight:700,padding:'2px 5px',borderRadius:3,cursor:'pointer',whiteSpace:'nowrap'}} onClick={() => setEditingQty(true)}>
              {qty.toLocaleString()}
            </span>
          )}
          {entry.priority > 0 && (
            <span style={{background:'#6b7280',color:'white',fontSize:8,padding:'0 3px',borderRadius:2}}>P{entry.priority}</span>
          )}
        </div>
      </div>
    </div>
  );
};

// ---- Intake Queue Card ----
const IntakeCard = ({ item, dragging, onDragStart, onDragEnd, onIssueCutList }) => {
  const isStock = item.is_stock_run;
  const hasPriority = !!item.requested_delivery_date;
  const invOnHand = item.inventory_on_hand || 0;
  const qty = item.quantity || 0;

  return (
    <div
      draggable
      onDragStart={onDragStart}
      onDragEnd={onDragEnd}
      style={{
        background: hasPriority ? '#fff7ed' : 'white',
        border: hasPriority ? '2px solid #f97316' : '1.5px solid #e2e8f0',
        borderRadius: 6,
        padding: '7px 8px',
        cursor: 'grab',
        opacity: dragging ? 0.5 : 1,
        userSelect: 'none',
        marginBottom: 6,
        transition: 'box-shadow 0.1s'
      }}
    >
      <div style={{display:'flex',alignItems:'flex-start',justifyContent:'space-between',gap:4}}>
        <div style={{flex:1,minWidth:0}}>
          <div style={{display:'flex',alignItems:'center',gap:3,flexWrap:'wrap',marginBottom:2}}>
            {isStock && <span style={{background:'#f59e0b',color:'white',fontSize:8,fontWeight:700,padding:'0 3px',borderRadius:2}}>STOCK</span>}
            {hasPriority && <span style={{background:'#ef4444',color:'white',fontSize:8,fontWeight:700,padding:'0 3px',borderRadius:2}}>⚡PRIORITY</span>}
          </div>
          <div style={{fontWeight:700,color:'#07324C',fontSize:11,overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}}>{item.order_number}</div>
          <div style={{color:'#6b7280',fontSize:10,overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}}>{item.sku_code || item.product_name || '—'}</div>
          {item.client_name && <div style={{color:'#9ca3af',fontSize:9,overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}}>{item.client_name}</div>}
          {item.requested_delivery_date && (
            <div style={{color:'#ef4444',fontSize:9,fontWeight:600,marginTop:1}}>Due: {item.requested_delivery_date}</div>
          )}
          {invOnHand > 0 && (
            <div style={{color:'#2563eb',fontSize:9,marginTop:1}}>{invOnHand.toLocaleString()} on hand</div>
          )}
        </div>
        <div>
          <span style={{background:'#16a34a',color:'white',fontSize:10,fontWeight:700,padding:'3px 6px',borderRadius:4,whiteSpace:'nowrap'}}>
            {qty.toLocaleString()}
          </span>
        </div>
      </div>
      {item.order_status === 'T' && onIssueCutList && (
        <div style={{marginTop:5,paddingTop:5,borderTop:'1px solid #e2e8f0'}}>
          <button
            onClick={e => { e.stopPropagation(); e.preventDefault(); onIssueCutList(item); }}
            onDragStart={e => e.stopPropagation()}
            draggable={false}
            style={{
              width:'100%',background:'#f59e0b',color:'white',border:'none',
              borderRadius:4,padding:'3px 6px',fontSize:9,fontWeight:700,
              cursor:'pointer',display:'flex',alignItems:'center',justifyContent:'center',gap:3
            }}
          >
            ✂ Issue Cut List
          </button>
        </div>
      )}
    </div>
  );
};

// ---- Drop Zone (machine/day cell) ----
const DropZone = ({ children, onDrop, isClosed, machineId, dateStr }) => {
  const [dragOver, setDragOver] = useState(false);

  if (isClosed) {
    return (
      <td style={{background:'#f3f4f6',borderRight:'1px solid #e2e8f0',borderBottom:'1px solid #e2e8f0',minWidth:110,verticalAlign:'top',padding:'4px'}}>
        <div style={{color:'#9ca3af',fontSize:9,textAlign:'center',marginTop:8}}>CLOSED</div>
      </td>
    );
  }

  return (
    <td
      style={{
        background: dragOver ? '#eff6ff' : 'white',
        border: dragOver ? '2px solid #3b82f6' : '1px solid transparent',
        borderRight: '1px solid #e2e8f0',
        borderBottom: '1px solid #e2e8f0',
        minWidth: 110,
        verticalAlign: 'top',
        padding: '4px',
        transition: 'background 0.1s'
      }}
      onDragOver={e => { e.preventDefault(); setDragOver(true); }}
      onDragLeave={() => setDragOver(false)}
      onDrop={e => {
        e.preventDefault();
        setDragOver(false);
        try {
          const data = JSON.parse(e.dataTransfer.getData('text/plain'));
          onDrop(dateStr, machineId, data);
        } catch {}
      }}
    >
      {children}
    </td>
  );
};

// ---- Machine Column Header ----
const MachineColHeader = ({ machine, slot }) => {
  const max = machine.max_units_per_day || 0;
  const total = slot?.total || 0;
  const over = slot?.over_capacity;

  return (
    <div style={{textAlign:'center',padding:'2px 0'}}>
      <div style={{fontWeight:700,fontSize:10,color: over ? '#ef4444' : '#07324C',whiteSpace:'nowrap',overflow:'hidden',textOverflow:'ellipsis'}}>
        {machine.name}
      </div>
      {max > 0 && (
        <div style={{fontSize:8,color:'#9ca3af'}}>Max: {max.toLocaleString()}</div>
      )}
      <div style={{
        fontSize: 9, fontWeight: 700,
        color: over ? '#ef4444' : '#16a34a',
        background: over ? '#fef2f2' : '#f0fdf4',
        borderRadius: 3, padding: '1px 4px', marginTop: 2
      }}>
        {total.toLocaleString()} {over ? '⚠ OVER' : ''}
      </div>
    </div>
  );
};

const PlanningBoard = ({ token }) => {
  const [weekOffset, setWeekOffset] = useState(0);
  const [activeTab, setActiveTab] = useState('viking'); // 'viking' | 'handmade' | 'dtl' | 'crates'
  const [planData, setPlanData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [draggingItem, setDraggingItem] = useState(null);
  const [showStockModal, setShowStockModal] = useState(false);
  const [splitEntry, setSplitEntry] = useState(null);
  const [zones, setZones] = useState([]);

  // Compute week start (Monday) from offset
  const getWeekStart = (offset) => {
    const now = new Date();
    const day = now.getDay();
    const mon = new Date(now);
    mon.setDate(now.getDate() - (day === 0 ? 6 : day - 1) + offset * 7);
    mon.setHours(0,0,0,0);
    return mon;
  };

  const fmtDate = d => {
    const y = d.getFullYear();
    const m = String(d.getMonth()+1).padStart(2,'0');
    const dd = String(d.getDate()).padStart(2,'0');
    return `${y}-${m}-${dd}`;
  };

  const weekStart = getWeekStart(weekOffset);
  const weekLabel = weekStart.toLocaleDateString('en-AU', {day:'numeric', month:'short'}) +
    ' — ' +
    new Date(weekStart.getTime() + 5*86400000).toLocaleDateString('en-AU', {day:'numeric', month:'short', year:'numeric'});

  const loadRef = useRef(null);
  loadRef.current = async () => {
    setLoading(true);
    try {
      const ws = fmtDate(weekStart);
      const endpoint = `/planning/${activeTab}?week_start=${ws}`;
      const [data, zs] = await Promise.all([
        api(endpoint, {}, token),
        api('/zones', {}, token)
      ]);
      setPlanData(data);
      setZones(zs);
    } catch(e) { toast(e.message, 'error'); }
    setLoading(false);
  };

  useEffect(() => { loadRef.current(); }, [weekOffset, activeTab, token]);
  const reload = () => loadRef.current();

  // Drop from intake queue → machine/day
  const handleDrop = async (dateStr, stationId, dragData) => {
    if (!dragData || !dragData.orderItemId) return;
    try {
      const zoneId = planData?.zone?.id;
      await api('/schedule', {
        method: 'POST',
        body: JSON.stringify({
          order_id: dragData.orderId,
          order_item_id: dragData.orderItemId,
          zone_id: zoneId,
          station_id: stationId,
          scheduled_date: dateStr,
          planned_quantity: dragData.quantity
        })
      }, token);
      toast('Scheduled — in docking', 'success');
      reload();
    } catch(e) { toast(e.message, 'error'); }
  };

  const removeEntry = async (entryId) => {
    try {
      await api(`/schedule/${entryId}`, {method:'DELETE'}, token);
      toast('Removed', 'info');
      reload();
    } catch(e) { toast(e.message, 'error'); }
  };

  const editQty = async (entryId, qty) => {
    try {
      await api(`/schedule/${entryId}`, {
        method: 'PUT',
        body: JSON.stringify({ planned_quantity: qty })
      }, token);
      reload();
    } catch(e) { toast(e.message, 'error'); }
  };

  const toggleCloseDay = async (dateStr, isClosed) => {
    const zoneId = planData?.zone?.id;
    const method = isClosed ? 'DELETE' : 'POST';
    try {
      await api('/planning/close-day', {
        method,
        body: JSON.stringify({ zone_id: zoneId, closed_date: dateStr })
      }, token);
      reload();
    } catch(e) { toast(e.message, 'error'); }
  };

  const machines = planData?.machines || [];
  const days = planData?.days || [];
  const intakeQueue = planData?.intake_queue || [];
  const dockingQueue = planData?.docking_queue || [];
  const [dockingCollapsed, setDockingCollapsed] = useState(false);

  const isViking = activeTab === 'viking';
  const zoneLabel = {viking:'Viking', handmade:'Handmade', dtl:'DTL', crates:'Crates'}[activeTab] || activeTab;
  const colLabel = {viking:'Machine', handmade:'Table', dtl:'Centre', crates:'Station'}[activeTab] || 'Station';

  // Determine max rows needed across all machines/days
  const maxRows = Math.max(1, ...days.flatMap(d =>
    machines.map(m => (d.machine_slots?.[m.id]?.entries?.length || 0))
  ));

  return (
    <div style={{height:'calc(100vh - 0px)',display:'flex',flexDirection:'column',overflow:'hidden'}}>
      {/* Toolbar */}
      <div style={{background:'#07324C',padding:'10px 16px',display:'flex',alignItems:'center',gap:10,flexShrink:0,flexWrap:'wrap'}}>
        <div style={{color:'white',fontWeight:900,fontSize:16,marginRight:4}}>{zoneLabel} Planning Board</div>

        {/* Zone tabs */}
        <div style={{display:'flex',gap:2,background:'rgba(255,255,255,0.1)',borderRadius:6,padding:2}}>
          {['viking','handmade','dtl','crates'].map(t => (
            <button key={t} onClick={() => setActiveTab(t)} style={{
              padding:'4px 12px', borderRadius:5, border:'none', cursor:'pointer', fontSize:11, fontWeight:700,
              background: activeTab===t ? 'white' : 'transparent',
              color: activeTab===t ? '#07324C' : 'rgba(255,255,255,0.8)'
            }}>{{viking:'Viking',handmade:'Handmade',dtl:'DTL',crates:'Crates'}[t]}</button>
          ))}
        </div>

        <div style={{borderLeft:'1px solid rgba(255,255,255,0.2)',height:20,margin:'0 4px'}}></div>

        {/* Week nav */}
        <button onClick={() => setWeekOffset(w=>w-1)} style={{background:'rgba(255,255,255,0.15)',border:'none',color:'white',borderRadius:5,padding:'4px 10px',cursor:'pointer',fontSize:12,fontWeight:700}}>‹ Prev</button>
        <span style={{color:'white',fontSize:11,fontWeight:600,minWidth:180,textAlign:'center'}}>{weekLabel}</span>
        <button onClick={() => setWeekOffset(w=>w+1)} style={{background:'rgba(255,255,255,0.15)',border:'none',color:'white',borderRadius:5,padding:'4px 10px',cursor:'pointer',fontSize:12,fontWeight:700}}>Next ›</button>
        <button onClick={() => setWeekOffset(0)} style={{background:'rgba(255,255,255,0.15)',border:'none',color:'white',borderRadius:5,padding:'4px 10px',cursor:'pointer',fontSize:11}}>This Week</button>

        <div style={{borderLeft:'1px solid rgba(255,255,255,0.2)',height:20,margin:'0 4px'}}></div>

        {/* Actions */}
        <button onClick={() => setShowStockModal(true)} style={{background:'#f59e0b',border:'none',color:'white',borderRadius:5,padding:'4px 12px',cursor:'pointer',fontSize:11,fontWeight:700}}>
          + Stock Run
        </button>
        <button onClick={reload} style={{background:'rgba(255,255,255,0.15)',border:'none',color:'white',borderRadius:5,padding:'4px 10px',cursor:'pointer',fontSize:11}}>
          ↺ Refresh
        </button>
      </div>

      {loading ? (
        <div style={{flex:1,display:'flex',alignItems:'center',justifyContent:'center'}}>
          <Spinner />
        </div>
      ) : (
        <div style={{flex:1,display:'flex',overflow:'hidden'}}>

          {/* LEFT: Intake Queue */}
          <div style={{
            width: 210,
            flexShrink: 0,
            background: '#f8fafc',
            borderRight: '2px solid #e2e8f0',
            display: 'flex',
            flexDirection: 'column',
            overflow: 'hidden'
          }}>
            <div style={{background:'#07324C',color:'white',padding:'8px 10px',fontSize:11,fontWeight:700,flexShrink:0}}>
              Intake Queue
            </div>
            <div style={{padding:'6px 8px',background:'#f1f5f9',borderBottom:'1px solid #e2e8f0',flexShrink:0}}>
              <div style={{fontSize:10,color:'#6b7280',lineHeight:1.4}}>
                Work orders ready to be dropped into a scheduled day slot
              </div>
              <div style={{fontSize:10,color:'#374151',fontWeight:600,marginTop:3}}>
                {intakeQueue.length} item{intakeQueue.length!==1?'s':''} in queue
              </div>
            </div>
            <div style={{flex:1,overflowY:'auto',padding:'6px 8px'}}>
              {intakeQueue.length === 0 ? (
                <div style={{textAlign:'center',color:'#9ca3af',fontSize:11,padding:'24px 8px'}}>
                  <div style={{fontSize:24,marginBottom:8}}>✓</div>
                  All work orders scheduled
                </div>
              ) : intakeQueue.map(item => (
                <IntakeCard
                  key={item.id}
                  item={item}
                  dragging={draggingItem === item.id}
                  onDragStart={e => {
                    e.dataTransfer.setData('text/plain', JSON.stringify({
                      orderItemId: item.id,
                      orderId: item.order_id,
                      quantity: item.quantity,
                      label: item.order_number
                    }));
                    setDraggingItem(item.id);
                  }}
                  onDragEnd={() => setDraggingItem(null)}
                  onIssueCutList={async (item) => {
                    try {
                      await api(`/orders/${item.order_id}/verify`, { method: 'PUT' }, token);
                      toast('Cut list issued — order moved to docking', 'success');
                      loadRef.current();
                    } catch(e) { toast(e.message, 'error'); }
                  }}
                />
              ))}
            </div>

            {/* Docking Queue */}
            <div style={{borderTop:'2px solid #bfdbfe',flexShrink:0}}>
              <div
                onClick={() => setDockingCollapsed(c => !c)}
                style={{
                  background:'#1e40af',color:'white',padding:'7px 10px',fontSize:11,
                  fontWeight:700,cursor:'pointer',display:'flex',alignItems:'center',
                  justifyContent:'space-between',userSelect:'none'
                }}
              >
                <span>🚢 Jobs in Docking ({dockingQueue.length})</span>
                <span style={{fontSize:10}}>{dockingCollapsed ? '▼' : '▲'}</span>
              </div>
              {!dockingCollapsed && (
                <div style={{maxHeight:260,overflowY:'auto',padding:'6px 8px',background:'#eff6ff'}}>
                  {dockingQueue.length === 0 ? (
                    <div style={{textAlign:'center',color:'#93c5fd',fontSize:10,padding:'16px 8px'}}>
                      No jobs currently in docking
                    </div>
                  ) : dockingQueue.map(item => (
                    <div key={item.id} style={{
                      background:'white',border:'1px solid #bfdbfe',borderLeft:'3px solid #2563eb',
                      borderRadius:5,padding:'6px 8px',marginBottom:5
                    }}>
                      <div style={{fontWeight:700,color:'#1e3a8a',fontSize:11,overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}}>
                        {item.order_number}
                      </div>
                      <div style={{color:'#6b7280',fontSize:9,overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}}>
                        {item.sku_code || item.product_name || '—'}
                      </div>
                      {item.client_name && (
                        <div style={{color:'#9ca3af',fontSize:9,overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}}>{item.client_name}</div>
                      )}
                      <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginTop:4}}>
                        <span style={{background:'#2563eb',color:'white',fontSize:9,fontWeight:700,padding:'1px 5px',borderRadius:3}}>
                          {(item.quantity||0).toLocaleString()}
                        </span>
                        <button
                          onClick={async () => {
                            try {
                              await api(`/orders/${item.order_id}/docking-complete`, { method: 'PUT' }, token);
                              toast('Docking complete — ready for production scheduling', 'success');
                              loadRef.current();
                            } catch(e) { toast(e.message, 'error'); }
                          }}
                          style={{
                            background:'#16a34a',color:'white',border:'none',borderRadius:3,
                            padding:'2px 6px',fontSize:9,fontWeight:700,cursor:'pointer'
                          }}
                        >
                          ✓ Docking Complete
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
          <div style={{flex:1,overflow:'auto',background:'white'}}>
            {machines.length === 0 ? (
              <div style={{padding:24,color:'#9ca3af',textAlign:'center'}}>No machines/stations configured for this zone</div>
            ) : (
              <table style={{borderCollapse:'collapse',width:'max-content',minWidth:'100%'}}>
                <thead>
                  {/* Row 1: Day headers */}
                  <tr>
                    <th style={{
                      background:'#07324C',color:'white',padding:'6px 8px',
                      fontSize:10,fontWeight:700,whiteSpace:'nowrap',
                      position:'sticky',top:0,left:0,zIndex:30,
                      borderRight:'2px solid rgba(255,255,255,0.3)',
                      minWidth:60
                    }}>
                      {colLabel}
                    </th>
                    {days.map(day => (
                      <th key={day.date} colSpan={machines.length} style={{
                        background: day.is_closed ? '#6b7280' : '#07324C',
                        color: 'white',
                        padding: '4px 8px',
                        fontSize: 11,
                        fontWeight: 700,
                        borderLeft: '2px solid rgba(255,255,255,0.2)',
                        position: 'sticky',
                        top: 0,
                        zIndex: 20,
                        textAlign: 'center',
                        minWidth: machines.length * 110
                      }}>
                        <div style={{display:'flex',alignItems:'center',justifyContent:'center',gap:8,flexWrap:'wrap'}}>
                          <span>{day.day_name} {day.date.slice(5)}</span>
                          {day.is_closed && <span style={{background:'rgba(255,255,255,0.2)',fontSize:9,padding:'1px 5px',borderRadius:3}}>CLOSED</span>}
                          {!day.is_closed && day.total_planned > 0 && (
                            <span style={{background:'rgba(22,163,74,0.4)',fontSize:9,padding:'1px 5px',borderRadius:3,whiteSpace:'nowrap'}}>
                              {day.total_planned.toLocaleString()} planned
                            </span>
                          )}
                          <label style={{display:'flex',alignItems:'center',gap:3,cursor:'pointer',fontSize:9,color:'rgba(255,255,255,0.8)'}}>
                            <input type="checkbox" checked={day.is_closed} onChange={() => toggleCloseDay(day.date, day.is_closed)} style={{cursor:'pointer',width:11,height:11}} />
                            Close
                          </label>
                        </div>
                      </th>
                    ))}
                  </tr>

                  {/* Row 2: Machine sub-headers */}
                  <tr>
                    <th style={{
                      background:'#f1f5f9',padding:'4px 6px',fontSize:9,color:'#6b7280',
                      position:'sticky',top:32,left:0,zIndex:30,
                      borderRight:'2px solid #cbd5e1',borderBottom:'2px solid #cbd5e1'
                    }}>
                      Priority
                    </th>
                    {days.map(day =>
                      machines.map(machine => {
                        const slot = day.machine_slots?.[machine.id] || {};
                        const over = slot.over_capacity;
                        return (
                          <th key={`${day.date}-${machine.id}`} style={{
                            background: over ? '#fef2f2' : '#f8fafc',
                            padding: '3px 4px',
                            borderLeft: '1px solid #e2e8f0',
                            borderBottom: '2px solid #cbd5e1',
                            position: 'sticky',
                            top: 32,
                            zIndex: 10,
                            minWidth: 110,
                            maxWidth: 140
                          }}>
                            <MachineColHeader machine={machine} slot={slot} />
                          </th>
                        );
                      })
                    )}
                  </tr>
                </thead>

                <tbody>
                  {/* Dynamic rows — one row per work order slot */}
                  {Array.from({length: maxRows + 1}, (_, rowIdx) => (
                    <tr key={rowIdx} style={{minHeight: 36}}>
                      {/* Row label */}
                      <td style={{
                        background:'#f8fafc',padding:'4px 6px',fontSize:9,color:'#9ca3af',
                        fontWeight:700,textAlign:'center',
                        position:'sticky',left:0,zIndex:5,
                        borderRight:'2px solid #cbd5e1',borderBottom:'1px solid #f1f5f9',
                        minWidth:60
                      }}>
                        {rowIdx === 0 ? '—' : rowIdx}
                      </td>
                      {days.map(day => {
                      // --- CLOSED DAY: condensed single cell per machine ---
                      if (day.is_closed) {
                        return machines.map(machine => {
                          const slot = day.machine_slots?.[machine.id] || {};
                          const total = slot.total || 0;
                          const count = (slot.entries || []).length;
                          return (
                            <td key={`${day.date}-${machine.id}-${rowIdx}-closed`} style={{
                              padding: '3px',
                              borderLeft: '1px solid #e2e8f0',
                              borderBottom: '1px solid #f1f5f9',
                              minWidth: 110,
                              background: '#f1f5f9'
                            }}>
                              {rowIdx === 0 && total > 0 ? (
                                <div style={{padding:'4px 6px',textAlign:'center',color:'#64748b',fontSize:10,fontStyle:'italic'}}>
                                  <span style={{fontWeight:700}}>{total.toLocaleString()}</span> units
                                  <div style={{fontSize:9,color:'#94a3b8',marginTop:1}}>{count} job{count !== 1 ? 's' : ''}</div>
                                </div>
                              ) : rowIdx === 0 ? (
                                <div style={{padding:'4px 6px',textAlign:'center',color:'#cbd5e1',fontSize:10,fontStyle:'italic'}}>Closed</div>
                              ) : null}
                            </td>
                          );
                        });
                      }
                      // --- OPEN DAY: normal rendering ---
                      return machines.map(machine => {
                          const slot = day.machine_slots?.[machine.id] || {};
                          const entries = slot.entries || [];
                          const entry = entries[rowIdx];

                          // Only last row + 1 is a true drop zone
                          const isDropRow = rowIdx === entries.length;

                          if (entry) {
                            return (
                              <td key={`${day.date}-${machine.id}-${rowIdx}`} style={{
                                padding: '3px 3px',
                                borderLeft: '1px solid #e2e8f0',
                                borderBottom: '1px solid #f1f5f9',
                                verticalAlign: 'top',
                                minWidth: 110
                              }}>
                                <WOCard
                                  entry={entry}
                                  onRemove={removeEntry}
                                  onSplit={setSplitEntry}
                                  onEditQty={editQty}
                                  onDockingComplete={async (e) => {
                                    try {
                                      await api(`/orders/${e.order_id}/docking-complete`, { method: 'PUT' }, token);
                                      toast('Docking complete — released for production', 'success');
                                      loadRef.current();
                                    } catch(err) { toast(err.message, 'error'); }
                                  }}
                                />
                              </td>
                            );
                          }

                          if (isDropRow) {
                            return (
                              <DropZone
                                key={`${day.date}-${machine.id}-drop`}
                                machineId={machine.id}
                                dateStr={day.date}
                                isClosed={day.is_closed}
                                onDrop={handleDrop}
                              >
                                <div style={{height:36,display:'flex',alignItems:'center',justifyContent:'center',color:'#d1d5db',fontSize:9}}>
                                  drop here
                                </div>
                              </DropZone>
                            );
                          }

                          return (
                            <td key={`${day.date}-${machine.id}-${rowIdx}-empty`} style={{
                              padding: '3px',
                              borderLeft: '1px solid #e2e8f0',
                              borderBottom: '1px solid #f1f5f9',
                              minWidth: 110,
                              background: day.is_closed ? '#f9fafb' : 'white'
                            }} />
                          );
                        })
                      })}
                    </tr>
                  ))}

                  {/* Extra drop row at bottom */}
                  <tr>
                    <td style={{
                      background:'#f8fafc',padding:'4px 6px',fontSize:9,color:'#9ca3af',
                      position:'sticky',left:0,zIndex:5,
                      borderRight:'2px solid #cbd5e1',borderBottom:'1px solid #f1f5f9',textAlign:'center'
                    }}>+</td>
                    {days.map(day =>
                      machines.map(machine => (
                        day.is_closed ? (
                          <td key={`${day.date}-${machine.id}-extra-closed`} style={{padding:3,borderLeft:'1px solid #e2e8f0',borderBottom:'1px solid #f1f5f9',minWidth:110,background:'#f1f5f9'}} />
                        ) : (
                        <DropZone
                          key={`${day.date}-${machine.id}-extra`}
                          machineId={machine.id}
                          dateStr={day.date}
                          isClosed={day.is_closed}
                          onDrop={handleDrop}
                        >
                          <div style={{height:28,display:'flex',alignItems:'center',justifyContent:'center',color:'#d1d5db',fontSize:9}}>
                            +
                          </div>
                        </DropZone>
                        )
                      ))
                    )}
                  </tr>
                </tbody>
              </table>
            )}
          </div>
        </div>
      )}

      {/* Modals */}
      {showStockModal && (
        <StockRunModal token={token} zones={zones} onClose={() => setShowStockModal(false)} onCreated={reload} />
      )}
      {splitEntry && (
        <SplitModal entry={splitEntry} token={token} onClose={() => setSplitEntry(null)} onSplit={reload} />
      )}
    </div>
  );
};


// ═══════════════════════════════════════════════════════════════
// PRODUCTION MANAGER — Station Allocation (primarily Handmade)
// Prod Manager assigns scheduled work orders to specific tables/stations
// ═══════════════════════════════════════════════════════════════
const ProductionManager = ({ token }) => {
  const [zones, setZones] = useState([]);
  const [activeZone, setActiveZone] = useState(null);
  const [stations, setStations] = useState([]);
  const [schedule, setSchedule] = useState([]);
  const [loading, setLoading] = useState(true);
  const [dateFilter, setDateFilter] = useState(localDateStr());
  const [dockingOrders, setDockingOrders] = useState([]);

  const activeZoneObj = zones.find(z => z.id === activeZone);
  const zoneStations = stations.filter(s => s.zone_id === activeZone);

  // Stable data loader using ref to avoid dep loops
  const loadRef = useRef(null);
  loadRef.current = async () => {
    setLoading(true);
    try {
      const [zs, sched, docking] = await Promise.all([
        api('/zones', {}, token),
        api(`/schedule?date=${dateFilter}`, {}, token),
        api('/orders?status=C', {}, token),
      ]);
      setZones(zs);
      // Default to Handmade zone
      setActiveZone(prev => {
        if (prev) return prev;
        const hmp = zs.find(z => z.code === 'HMP');
        return hmp ? hmp.id : (zs.length ? zs[0].id : null);
      });
      setSchedule(sched);
      setDockingOrders(docking || []);
      const allSt = [];
      for (const z of zs) { if (z.stations) allSt.push(...z.stations); }
      setStations(allSt);
    } catch (e) { toast(e.message, 'error'); }
    setLoading(false);
  };

  useEffect(() => { loadRef.current(); }, [dateFilter, token]);

  const reload = () => loadRef.current();

  const assignStation = async (entryId, stationId) => {
    try {
      await api(`/schedule/${entryId}`, {
        method: 'PUT',
        body: JSON.stringify({ station_id: stationId || null })
      }, token);
      toast(stationId ? 'Station assigned' : 'Station removed', 'success');
      reload();
    } catch (e) { toast(e.message, 'error'); }
  };

  const moveStatus = async (entryId, status) => {
    try {
      await api(`/schedule/${entryId}`, {
        method: 'PUT',
        body: JSON.stringify({ status })
      }, token);
      toast(`Status → ${status}`, 'success');
      reload();
    } catch (e) { toast(e.message, 'error'); }
  };

  const zoneEntries = schedule.filter(s => s.zone_id === activeZone);
  const unassigned = zoneEntries.filter(s => !s.station_id);
  const stationLabel = activeZoneObj?.code === 'HMP' ? 'Table' : activeZoneObj?.code === 'VIK' ? 'Machine' : 'Station';

  // Navigate dates
  const shiftDate = (days) => {
    const d = new Date(dateFilter);
    d.setDate(d.getDate() + days);
    setDateFilter(localDateStr(d));
  };
  const isToday = dateFilter === localDateStr();
  const dateLabelStr = new Date(dateFilter + 'T00:00:00').toLocaleDateString('en-AU',{weekday:'long',day:'numeric',month:'long',year:'numeric'});

  return (
    <div className="p-6 space-y-4">
      {/* Header */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-2xl font-black text-[#07324C]">Station Allocation</h1>
          <p className="text-xs text-gray-500 mt-0.5">Production Manager — assign work orders to stations/tables</p>
        </div>
        <div className="flex items-center gap-2">
          <button className="btn btn-secondary btn-sm" onClick={()=>shiftDate(-1)}>‹ Prev Day</button>
          <span className={`text-sm font-semibold px-2 ${isToday?'text-[#07324C]':'text-gray-600'}`}>{dateLabelStr}</span>
          <button className="btn btn-secondary btn-sm" onClick={()=>shiftDate(1)}>Next Day ›</button>
          <button className="btn btn-outline btn-sm" onClick={()=>setDateFilter(localDateStr())}>Today</button>
          <button className="btn btn-outline btn-sm" onClick={reload}>Refresh</button>
        </div>
      </div>

      {/* Zone tabs */}
      <div className="tab-bar">
        {zones.map(z => (
          <div key={z.id} className={`tab-item ${activeZone===z.id?'active':''}`} onClick={()=>setActiveZone(z.id)}>{z.name}</div>
        ))}
      </div>

      {loading ? <div className="flex justify-center py-12"><Spinner /></div> : (
        <>
          {/* Docking Queue Panel */}
          {dockingOrders.length > 0 && (
            <div className="hp-card" style={{borderTop:'3px solid #1d4ed8'}}>
              <h3 className="text-xs font-bold text-blue-700 uppercase tracking-wider mb-2">
                🚢 Docking Queue — {dockingOrders.length} order{dockingOrders.length!==1?'s':''} awaiting docking completion
              </h3>
              <p className="text-xs text-gray-400 mb-3">These orders have cut lists issued and are currently being docked/prepared. Mark complete when ready for production scheduling.</p>
              <div className="flex gap-3 overflow-x-auto pb-1" style={{scrollSnapType:'x mandatory'}}>
                {dockingOrders.map(order => (
                  <div key={order.id} style={{
                    minWidth:200,maxWidth:220,background:'#eff6ff',border:'1px solid #bfdbfe',
                    borderLeft:'4px solid #2563eb',borderRadius:8,padding:'10px 12px',flexShrink:0,
                    scrollSnapAlign:'start'
                  }}>
                    <div style={{fontWeight:800,color:'#1e3a8a',fontSize:12,marginBottom:2}}>{order.order_number}</div>
                    <div style={{fontSize:11,color:'#6b7280',marginBottom:1}}>{order.client_name||'—'}</div>
                    <div style={{fontSize:10,color:'#9ca3af',marginBottom:4}}>{order.item_count||0} item{order.item_count!==1?'s':''} · {(order.total_qty||0).toLocaleString()} units</div>
                    <button
                      onClick={async () => {
                        try {
                          await api(`/orders/${order.id}/docking-complete`, { method: 'PUT' }, token);
                          toast('Docking complete — ready for production scheduling', 'success');
                          reload();
                        } catch(e) { toast(e.message, 'error'); }
                      }}
                      style={{
                        width:'100%',background:'#16a34a',color:'white',border:'none',
                        borderRadius:5,padding:'5px 8px',fontSize:10,fontWeight:700,cursor:'pointer'
                      }}
                    >
                      ✓ Complete Docking
                    </button>
                  </div>
                ))}
              </div>
            </div>
          )}
          <div className="grid gap-4" style={{gridTemplateColumns: 'minmax(240px,1fr) 3fr'}}>
          {/* Unassigned queue */}
          <div className="hp-card">
            <h3 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-1">Unassigned Work</h3>
            <div className="text-xs text-gray-400 mb-3">{unassigned.length} order{unassigned.length!==1?'s':''} need {stationLabel.toLowerCase()} assignment</div>
            {unassigned.length === 0 ? <Empty msg="All work assigned — or no work scheduled for this date/zone" /> : (
              <div className="space-y-2" style={{maxHeight:'calc(100vh - 320px)',overflowY:'auto'}}>
                {unassigned.map(entry => (
                  <div key={entry.id} style={{background:'#fffbeb',border:'1px solid #fbbf24',borderRadius:8,padding:'8px 10px'}}>
                    <div className="font-bold text-[#07324C]" style={{fontSize:12}}>{entry.order_number||'—'}</div>
                    <div className="text-gray-500" style={{fontSize:11}}>{entry.client_name||''}</div>
                    <div className="text-gray-500" style={{fontSize:11}}>{entry.product_name||entry.sku_code||'—'}</div>
                    <div className="text-gray-400" style={{fontSize:11}}>{entry.item_quantity||entry.planned_quantity||0} units</div>
                    <div style={{marginTop:6}}>
                      <select
                        style={{fontSize:11,padding:'4px 6px',borderRadius:6,border:'1px solid #d1d5db',width:'100%'}}
                        value=""
                        onChange={e => e.target.value && assignStation(entry.id, parseInt(e.target.value))}
                      >
                        <option value="">Assign {stationLabel}...</option>
                        {zoneStations.map(st => (
                          <option key={st.id} value={st.id}>{st.name}</option>
                        ))}
                      </select>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Station cards grid */}
          <div>
            <div className="grid gap-3" style={{gridTemplateColumns: `repeat(auto-fill, minmax(200px, 1fr))`}}>
              {zoneStations.map(station => {
                const stEntries = zoneEntries.filter(s => s.station_id === station.id);
                const hasWork = stEntries.length > 0;
                return (
                  <div key={station.id} className="hp-card" style={{borderTop: hasWork ? '3px solid #07324C' : '3px solid #e2e8f0', padding:'12px'}}>
                    <div className="flex items-center justify-between mb-2">
                      <div>
                        <div style={{fontWeight:800,fontSize:13,color:'#07324C'}}>{station.name}</div>
                        <div style={{fontSize:10,color:'#9ca3af'}}>{station.code}</div>
                      </div>
                      <div style={{
                        background: hasWork ? '#dcfce7' : '#f3f4f6',
                        color: hasWork ? '#166534' : '#9ca3af',
                        fontSize:10, fontWeight:700, padding:'2px 8px', borderRadius:10
                      }}>
                        {stEntries.length} job{stEntries.length!==1?'s':''}
                      </div>
                    </div>
                    {stEntries.length === 0 ? (
                      <div style={{fontSize:11,color:'#9ca3af',fontStyle:'italic',padding:'8px 0'}}>No work allocated</div>
                    ) : (
                      <div className="space-y-2">
                        {stEntries.map(entry => (
                          <div key={entry.id} style={{background:'#f8fafc',border:'1px solid #e2e8f0',borderRadius:6,padding:'6px 8px'}}>
                            <div className="flex items-start justify-between">
                              <div>
                                <div className="font-bold text-[#07324C]" style={{fontSize:11}}>{entry.order_number||'—'}</div>
                                <div className="text-gray-500" style={{fontSize:10}}>{entry.client_name||''}</div>
                                <div className="text-gray-500" style={{fontSize:10}}>{entry.product_name||entry.sku_code||'—'}</div>
                                <div className="text-gray-400" style={{fontSize:10}}>{entry.item_quantity||entry.planned_quantity||0} units</div>
                              </div>
                              <span style={{
                                fontSize:9,fontWeight:700,padding:'2px 6px',borderRadius:8,
                                background: entry.status==='in_progress' ? '#dbeafe' : entry.status==='completed' ? '#dcfce7' : '#f3f4f6',
                                color: entry.status==='in_progress' ? '#1d4ed8' : entry.status==='completed' ? '#166534' : '#6b7280'
                              }}>{entry.status||'planned'}</span>
                            </div>
                            <div className="flex gap-1 mt-2">
                              <button style={{fontSize:9,padding:'2px 6px',background:'#fee2e2',color:'#991b1b',borderRadius:4,cursor:'pointer',border:'none'}}
                                onClick={()=>assignStation(entry.id, null)}>Unassign</button>
                              {entry.status === 'planned' && (
                                <button style={{fontSize:9,padding:'2px 6px',background:'#dbeafe',color:'#1d4ed8',borderRadius:4,cursor:'pointer',border:'none'}}
                                  onClick={()=>moveStatus(entry.id,'in_progress')}>Start</button>
                              )}
                              {entry.status === 'in_progress' && (
                                <button style={{fontSize:9,padding:'2px 6px',background:'#dcfce7',color:'#166534',borderRadius:4,cursor:'pointer',border:'none'}}
                                  onClick={()=>moveStatus(entry.id,'completed')}>Complete</button>
                              )}
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        </div>
        </>
      )}
    </div>
  );
};

// ═══════════════════════════════════════════════════════════════
// FLOOR TABLET
// ═══════════════════════════════════════════════════════════════
const FloorTablet = ({ token, user }) => {
  const [view, setView] = useState('select'); // select | setup | production
  const [orders, setOrders] = useState([]);
  const [session, setSession] = useState(null);
  const [loading, setLoading] = useState(true);
  const [qty, setQty] = useState(0);
  const [target, setTarget] = useState(0);
  const [paused, setPaused] = useState(false);
  const [pauseReason, setPauseReason] = useState(null);
  const intervalRef = useRef(null);

  const loadOrders = useCallback(async () => {
    try {
      const data = await api('/orders?status=R', {}, token);
      const inProd = await api('/orders?status=P', {}, token);
      setOrders([...inProd, ...data]);
    } catch (e) { toast(e.message,'error'); }
    setLoading(false);
  }, [token]);

  useEffect(() => { loadOrders(); }, [loadOrders]);

  // Auto-refresh production session
  useEffect(() => {
    if (view === 'production' && session) {
      intervalRef.current = setInterval(async () => {
        try {
          const sessions = await api(`/production/sessions?status=active`, {}, token);
          const curr = sessions.find(s=>s.id===session.id);
          if (curr) { setQty(curr.produced_quantity||0); setPaused(false); }
          else {
            const paused = await api(`/production/sessions?status=paused`, {}, token);
            const p = paused.find(s=>s.id===session.id);
            if (p) { setQty(p.produced_quantity||0); setPaused(true); }
          }
        } catch {}
      }, 30000);
    }
    return () => clearInterval(intervalRef.current);
  }, [view, session, token]);

  const startSession = async (order) => {
    try {
      // Get first order item
      const items = await api(`/orders/${order.id}/items`, {}, token);
      if (!items.length) { toast('No items found for this order','warning'); return; }
      const item = items[0];
      const zones = await api('/zones', {}, token);
      const zone = zones.find(z=>z.id===item.zone_id) || zones[0];
      if (!zone?.stations?.length) { toast('No stations available','warning'); return; }
      const station = zone.stations[0];

      const s = await api('/production/sessions', {
        method: 'POST',
        body: JSON.stringify({
          order_item_id: item.id,
          station_id: station.id,
          zone_id: zone.id,
          target_quantity: item.quantity
        })
      }, token);
      // Update order status to P
      await api(`/orders/${order.id}/status`, {method:'PUT',body:JSON.stringify({status:'P'})}, token);
      setSession({ ...s, order, item, zone, station });
      setQty(s.produced_quantity || 0);
      setTarget(item.quantity);
      setView('production');
      toast('Production session started','success');
    } catch (e) { toast(e.message,'error'); }
  };

  const logQty = async (change) => {
    if (!session || paused) return;
    try {
      const res = await api(`/production/sessions/${session.id}/log`, {method:'PUT',body:JSON.stringify({quantity_change:change})}, token);
      setQty(res.running_total);
    } catch (e) { toast(e.message,'error'); }
  };

  const pauseSession = async (reason) => {
    try {
      await api(`/production/sessions/${session.id}/pause`, {method:'PUT',body:JSON.stringify({reason})}, token);
      setPaused(true); setPauseReason(reason);
      toast(`Paused: ${reason}`,'warning');
    } catch (e) { toast(e.message,'error'); }
  };

  const resumeSession = async () => {
    try {
      await api(`/production/sessions/${session.id}/resume`, {method:'PUT'}, token);
      setPaused(false); setPauseReason(null);
      toast('Production resumed','success');
    } catch (e) { toast(e.message,'error'); }
  };

  const completeSession = async () => {
    try {
      await api(`/production/sessions/${session.id}/complete`, {method:'PUT',body:JSON.stringify({produced_quantity:qty})}, token);
      await api(`/orders/${session.order.id}/status`, {method:'PUT',body:JSON.stringify({status:'F'})}, token);
      toast('Production complete! Order marked Finished.','success');
      setView('select');
      setSession(null);
      setQty(0);
      loadOrders();
    } catch (e) { toast(e.message,'error'); }
  };

  const pct = target > 0 ? Math.min(100, Math.round(qty/target*100)) : 0;
  const PAUSE_REASONS = ['material','cleaning','break','breakdown','forklift','urgent_changeover'];

  if (view === 'select') {
    return (
      <div className="p-6 space-y-4">
        <h1 className="text-2xl font-black text-[#07324C]">Production Floor</h1>
        <p className="text-gray-500 text-sm">Welcome, {user?.full_name}. Select a work order to begin.</p>

        {loading ? <div className="flex justify-center py-12"><Spinner /></div> : (
          orders.length === 0 ? <Empty msg="No orders assigned. Contact your supervisor." /> : (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {orders.map(o => {
                const pctProd = o.total_qty > 0 ? Math.min(100,Math.round((o.total_produced||0)/o.total_qty*100)) : 0;
                return (
                  <div key={o.id} className="hp-card cursor-pointer hover:shadow-lg transition-shadow" onClick={()=>startSession(o)}>
                    <div className="flex items-start justify-between mb-3">
                      <div>
                        <div className="text-lg font-black text-[#07324C]">{o.order_number}</div>
                        <div className="text-sm text-gray-500">{o.client_name}</div>
                      </div>
                      <StatusBadge status={o.status} />
                    </div>
                    <div className="text-xs text-gray-500 mb-3">{o.item_count} item{o.item_count!==1?'s':''} · {o.total_qty} units total</div>
                    <div className="progress-bar-bg h-2 mb-2">
                      <div className="progress-bar-fill" style={{width:`${pctProd}%`}}></div>
                    </div>
                    <div className="flex items-center justify-between text-xs">
                      <span className="text-gray-500">{o.total_produced||0} / {o.total_qty} produced</span>
                      <span className="font-bold text-[#07324C]">{pctProd}%</span>
                    </div>
                    <button className={`btn w-full justify-center mt-4 btn-lg ${o.status==='P'?'btn-warning':'btn-success'}`}>
                      {o.status==='P'?'Resume Production':'Start Production'}
                    </button>
                  </div>
                );
              })}
            </div>
          )
        )}
      </div>
    );
  }

  // Production screen
  return (
    <div className="p-4 space-y-4 max-w-2xl mx-auto">
      {/* Order banner */}
      <div style={{background:'#07324C',borderRadius:12,padding:'16px 20px',color:'white'}}>
        <div className="flex items-start justify-between">
          <div>
            <div style={{fontSize:22,fontWeight:900}}>{session?.order?.order_number}</div>
            <div style={{fontSize:14,color:'#ABBFC8',marginTop:2}}>{session?.item?.product_name||session?.item?.sku_code}</div>
            <div style={{fontSize:12,color:'#ABBFC8'}}>{session?.order?.client_name}</div>
          </div>
          <div style={{textAlign:'right'}}>
            <div style={{fontSize:11,color:'#ABBFC8'}}>Drawing</div>
            <div style={{fontWeight:700}}>{session?.item?.drawing_number||'N/A'}</div>
          </div>
        </div>
        {session?.order?.special_instructions && (
          <div style={{marginTop:10,padding:'8px 12px',background:'rgba(237,28,36,0.15)',borderRadius:6,borderLeft:'3px solid #ED1C24',fontSize:12}}>
            <strong>Note:</strong> {session.order.special_instructions}
          </div>
        )}
      </div>

      {/* Progress */}
      <div className="hp-card text-center">
        <div style={{fontSize:56,fontWeight:900,color:'#07324C',lineHeight:1}}>{qty.toLocaleString()}</div>
        <div style={{fontSize:16,color:'#5F545C',margin:'4px 0'}}>/ {target.toLocaleString()} units</div>
        <div className="progress-bar-bg h-4 my-4">
          <div className="progress-bar-fill" style={{width:`${pct}%`,height:'100%',background: pct>=100?'#22C55E':pct>=80?'#F59E0B':'#07324C'}}></div>
        </div>
        <div style={{fontSize:32,fontWeight:900,color: pct>=100?'#22C55E':pct>=80?'#F59E0B':'#07324C'}}>{pct}%</div>
      </div>

      {/* Pause banner */}
      {paused && (
        <div className="flex items-center justify-between p-4 bg-amber-50 border-2 border-amber-400 rounded-xl">
          <div>
            <div className="font-black text-amber-800 text-lg">PAUSED</div>
            <div className="text-amber-600 text-sm capitalize">{pauseReason}</div>
          </div>
          <button className="btn btn-success btn-xl" onClick={resumeSession}>▶ Resume</button>
        </div>
      )}

      {/* Count buttons */}
      {!paused && (
        <div className="flex gap-3">
          <button className="floor-btn floor-btn-add" onClick={()=>logQty(1)}>+1</button>
          <button className="floor-btn floor-btn-add10" onClick={()=>logQty(10)}>+10</button>
          <button className="floor-btn floor-btn-sub" onClick={()=>logQty(-1)}>−1</button>
        </div>
      )}

      {/* Pause reasons */}
      {!paused && (
        <div>
          <div className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-2">Pause Reason</div>
          <div className="flex flex-wrap gap-2">
            {PAUSE_REASONS.map(r => (
              <button key={r} className="pause-btn capitalize" onClick={()=>pauseSession(r)}>
                {r.replace('_',' ')}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Complete + switch */}
      <div className="flex gap-3">
        {pct >= 100 && (
          <button className="btn btn-success btn-xl flex-1 justify-center" onClick={completeSession}>
            ✓ Complete Order
          </button>
        )}
        <button className="btn btn-secondary btn-lg flex-1 justify-center" onClick={()=>{ setView('select'); }}>
          Switch Order
        </button>
        <button className="btn btn-outline btn-lg" onClick={()=>{ setView('select'); setSession(null); }}>
          Log Out
        </button>
      </div>
    </div>
  );
};

// ═══════════════════════════════════════════════════════════════
// QA DASHBOARD
// ═══════════════════════════════════════════════════════════════
const QADashboard = ({ token }) => {
  const [tab, setTab] = useState('pending');
  const [inspections, setInspections] = useState([]);
  const [finishedOrders, setFinishedOrders] = useState([]);
  const [loading, setLoading] = useState(true);
  const [inspecting, setInspecting] = useState(null);
  const [defectForm, setDefectForm] = useState({type:'rework',qty:1,desc:''});
  const [defects, setDefects] = useState([]);
  const [notes, setNotes] = useState('');

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const [insps, orders] = await Promise.all([
        api('/qa/inspections', {}, token),
        api('/orders?status=F', {}, token),
      ]);
      setInspections(insps);
      setFinishedOrders(orders);
    } catch (e) { toast(e.message,'error'); }
    setLoading(false);
  }, [token]);

  useEffect(() => { load(); }, [load]);

  const startInspection = async (order) => {
    try {
      const items = await api(`/orders/${order.id}/items`, {}, token);
      if (!items.length) { toast('No items','warning'); return; }
      const ins = await api('/qa/inspections', {
        method:'POST',
        body:JSON.stringify({ order_item_id:items[0].id, batch_size:items[0].quantity, inspection_type:'final' })
      }, token);
      setInspecting({inspection:ins, order, item:items[0]});
      setDefects([]);
      setNotes('');
    } catch (e) { toast(e.message,'error'); }
  };

  const addDefect = async () => {
    if (!inspecting) return;
    try {
      const d = await api(`/qa/inspections/${inspecting.inspection.id}/defects`, {
        method:'POST',
        body:JSON.stringify({defect_type:defectForm.type,quantity:defectForm.qty,description:defectForm.desc})
      }, token);
      setDefects(prev=>[...prev,d]);
      setDefectForm({type:'rework',qty:1,desc:''});
      toast('Defect logged','info');
    } catch (e) { toast(e.message,'error'); }
  };

  const approveAndRelease = async () => {
    try {
      await api(`/qa/inspections/${inspecting.inspection.id}/approve`, {method:'PUT'}, token);
      await api(`/orders/${inspecting.order.id}/status`, {method:'PUT',body:JSON.stringify({status:'dispatched'})}, token);
      toast('Approved & released to dispatch','success');
      setInspecting(null);
      load();
    } catch (e) { toast(e.message,'error'); }
  };

  return (
    <div className="p-6 space-y-4">
      <h1 className="text-2xl font-black text-[#07324C]">QA Dashboard</h1>

      <div className="tab-bar">
        <div className={`tab-item ${tab==='pending'?'active':''}`} onClick={()=>setTab('pending')}>
          Pending Inspections {finishedOrders.length>0&&<span className="ml-1.5 bg-red-500 text-white text-xs rounded-full px-2">{finishedOrders.length}</span>}
        </div>
        <div className={`tab-item ${tab==='audit'?'active':''}`} onClick={()=>setTab('audit')}>Audit Log</div>
      </div>

      {loading ? <div className="flex justify-center py-12"><Spinner /></div> : (
        <>
          {tab === 'pending' && !inspecting && (
            <div className="hp-card">
              <h2 className="text-sm font-bold text-gray-500 uppercase tracking-wider mb-4">Orders Ready for QA ({finishedOrders.length})</h2>
              {finishedOrders.length === 0 ? <Empty msg="No orders awaiting QA inspection" /> : (
                <div className="table-scroll">
                  <table className="hp-table">
                    <thead><tr><th>Order #</th><th>Client</th><th>Items</th><th>Total Qty</th><th>Finished At</th><th>Action</th></tr></thead>
                    <tbody>
                      {finishedOrders.map(o => (
                        <tr key={o.id}>
                          <td className="font-bold text-[#07324C]">{o.order_number}</td>
                          <td>{o.client_name}</td>
                          <td>{o.item_count}</td>
                          <td>{o.total_qty}</td>
                          <td className="text-xs text-gray-500">{o.updated_at?.split('T')[0]}</td>
                          <td><button className="btn btn-primary btn-sm" onClick={()=>startInspection(o)}>Inspect</button></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}

          {tab === 'pending' && inspecting && (
            <div className="hp-card">
              <div className="flex items-center justify-between mb-4">
                <h2 className="font-bold text-[#07324C] text-lg">Inspecting: {inspecting.order.order_number}</h2>
                <button className="btn btn-secondary btn-sm" onClick={()=>setInspecting(null)}>← Back</button>
              </div>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <div>
                  <h3 className="text-sm font-bold text-gray-500 uppercase tracking-wider mb-3">Batch Details</h3>
                  <div className="space-y-2 text-sm">
                    <div><span className="text-gray-400">Client:</span> <span className="font-medium">{inspecting.order.client_name}</span></div>
                    <div><span className="text-gray-400">Product:</span> <span className="font-medium">{inspecting.item.product_name}</span></div>
                    <div><span className="text-gray-400">Batch Size:</span> <span className="font-medium">{inspecting.item.quantity} units</span></div>
                    <div><span className="text-gray-400">Drawing:</span> <span className="font-medium">{inspecting.item.drawing_number||'N/A'}</span></div>
                  </div>

                  <h3 className="text-sm font-bold text-gray-500 uppercase tracking-wider mt-4 mb-3">Log Defect</h3>
                  <div className="space-y-2">
                    <select className="hp-input" value={defectForm.type} onChange={e=>setDefectForm(p=>({...p,type:e.target.value}))}>
                      <option value="rework">Rework</option>
                      <option value="2nds">2nds</option>
                      <option value="destroy">Destroy</option>
                    </select>
                    <input type="number" className="hp-input" min="1" value={defectForm.qty} onChange={e=>setDefectForm(p=>({...p,qty:parseInt(e.target.value)||1}))} placeholder="Quantity" />
                    <input className="hp-input" value={defectForm.desc} onChange={e=>setDefectForm(p=>({...p,desc:e.target.value}))} placeholder="Description…" />
                    <button className="btn btn-warning btn-sm" onClick={addDefect}>+ Log Defect</button>
                  </div>
                </div>

                <div>
                  <h3 className="text-sm font-bold text-gray-500 uppercase tracking-wider mb-3">Defects Logged ({defects.length})</h3>
                  {defects.length === 0 ? <p className="text-sm text-gray-400 mb-4">No defects logged.</p> : (
                    <div className="space-y-2 mb-4">
                      {defects.map((d,i) => (
                        <div key={i} className="flex items-center gap-3 p-2 bg-red-50 border border-red-100 rounded text-sm">
                          <span className="font-bold text-red-700 capitalize">{d.defect_type}</span>
                          <span className="text-red-600">×{d.quantity}</span>
                          <span className="text-gray-500 text-xs">{d.description}</span>
                        </div>
                      ))}
                    </div>
                  )}

                  <textarea className="hp-input mb-4" rows={3} value={notes} onChange={e=>setNotes(e.target.value)} placeholder="QA notes…" />

                  <button className="btn btn-success btn-lg w-full justify-center" onClick={approveAndRelease}>
                    <Icon name="CheckCircle" size={18}/> Approve & Release to Dispatch
                  </button>
                </div>
              </div>
            </div>
          )}

          {tab === 'audit' && (
            <div className="hp-card">
              <h2 className="text-sm font-bold text-gray-500 uppercase tracking-wider mb-4">QA Audit Log</h2>
              {inspections.length === 0 ? <Empty msg="No inspections recorded yet" /> : (
                <div className="table-scroll">
                  <table className="hp-table">
                    <thead><tr><th>Inspector</th><th>SKU</th><th>Type</th><th>Batch</th><th>Result</th><th>Date</th></tr></thead>
                    <tbody>
                      {inspections.map(i => (
                        <tr key={i.id}>
                          <td>{i.inspector_name||'—'}</td>
                          <td className="font-mono text-xs">{i.sku_code||'—'}</td>
                          <td className="capitalize">{i.inspection_type}</td>
                          <td>{i.batch_size||'—'}</td>
                          <td>{i.passed ? <span className="text-green-600 font-bold">Pass</span> : <span className="text-red-600 font-bold">Fail</span>}</td>
                          <td className="text-xs text-gray-500">{i.inspected_at?.split('T')[0]}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}
        </>
      )}
    </div>
  );
};

// ═══════════════════════════════════════════════════════════════
// DISPATCH DASHBOARD
// ═══════════════════════════════════════════════════════════════
const DispatchDashboard = ({ token }) => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [trucks, setTrucks] = useState([]);
  const [selectedDate, setSelectedDate] = useState(localDateStr());
  const [truckAssign, setTruckAssign] = useState({});
  const [dragOver, setDragOver] = useState(null);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const [d, deliveries] = await Promise.all([
        api(`/dispatch?date=${selectedDate}`, {}, token),
        api('/delivery-log', {}, token),
      ]);
      setData(d);
      // Extract unique trucks
      const uniqueTrucks = [...new Set(deliveries.filter(x=>x.truck_name).map(x=>x.truck_name))];
      setTrucks(uniqueTrucks.length ? uniqueTrucks : ['Truck A','Truck B','Truck C']);
    } catch (e) { toast(e.message,'error'); }
    setLoading(false);
  }, [token, selectedDate]);

  useEffect(() => { load(); }, [load]);

  const confirmCollection = async (orderId) => {
    try {
      await api(`/orders/${orderId}/status`, {method:'PUT',body:JSON.stringify({status:'collected'})}, token);
      toast('Marked as collected','success');
      load();
    } catch (e) { toast(e.message,'error'); }
  };

  const confirmDelivery = async (orderId) => {
    try {
      await api(`/orders/${orderId}/status`, {method:'PUT',body:JSON.stringify({status:'delivered'})}, token);
      toast('Marked as delivered','success');
      load();
    } catch (e) { toast(e.message,'error'); }
  };

  const assignTruck = (orderId, truck) => {
    setTruckAssign(p=>({...p,[orderId]:truck}));
  };

  return (
    <div className="p-6 space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-black text-[#07324C]">Dispatch</h1>
        <div className="flex items-center gap-2">
          <label className="hp-label m-0">Date:</label>
          <input type="date" className="hp-input" style={{width:160}} value={selectedDate} onChange={e=>setSelectedDate(e.target.value)} />
        </div>
      </div>

      {loading ? <div className="flex justify-center py-12"><Spinner /></div> : (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Deliveries */}
          <div className="hp-card">
            <h2 className="text-sm font-bold text-gray-500 uppercase tracking-wider mb-4">
              Deliveries — {selectedDate}
            </h2>
            {!data?.deliveries?.length ? <Empty msg="No deliveries scheduled for this date" /> : (
              <div className="space-y-3">
                {data.deliveries.map(d => (
                  <div key={d.id} className="dispatch-card p-4 border border-gray-200 rounded-xl hover:border-[#07324C] transition-colors"
                    draggable
                    onDragStart={e=>e.dataTransfer.setData('text/plain',JSON.stringify({orderId:d.order_id,label:d.order_number}))}
                  >
                    <div className="flex items-start justify-between mb-2">
                      <div>
                        <div className="font-bold text-[#07324C]">{d.order_number}</div>
                        <div className="text-sm text-gray-500">{d.company_name}</div>
                      </div>
                      <StatusBadge status={d.status || 'dispatched'} />
                    </div>
                    <div className="flex items-center gap-2 mt-3">
                      <select className="hp-input flex-1" style={{fontSize:12}}
                        value={truckAssign[d.id]||d.truck_name||''}
                        onChange={e=>assignTruck(d.id, e.target.value)}>
                        <option value="">Assign truck…</option>
                        {trucks.map(t=><option key={t}>{t}</option>)}
                      </select>
                      <button className="btn btn-success btn-sm" onClick={()=>confirmDelivery(d.order_id)}>✓ Delivered</button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Collections */}
          <div className="hp-card">
            <h2 className="text-sm font-bold text-gray-500 uppercase tracking-wider mb-4">Collections</h2>
            {!data?.collections?.length ? <Empty msg="No collections pending" /> : (
              <div className="space-y-3">
                {data.collections.map(o => (
                  <div key={o.id} className="p-4 border border-gray-200 rounded-xl">
                    <div className="flex items-start justify-between mb-3">
                      <div>
                        <div className="font-bold text-[#07324C]">{o.order_number}</div>
                        <div className="text-sm text-gray-500">{o.client_name}</div>
                      </div>
                      <StatusBadge status="F" />
                    </div>
                    <div className="flex gap-2">
                      <button className="btn btn-primary btn-sm flex-1 justify-center">Notify Customer</button>
                      <button className="btn btn-success btn-sm flex-1 justify-center" onClick={()=>confirmCollection(o.id)}>✓ Collected</button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      )}

      {/* Delivery log */}
      <DeliveryLog token={token} />
    </div>
  );
};

const DeliveryLog = ({ token }) => {
  const [logs, setLogs] = useState([]);
  useEffect(() => {
    api('/delivery-log', {}, token).then(setLogs).catch(()=>{});
  }, [token]);
  if (!logs.length) return null;
  return (
    <div className="hp-card">
      <h2 className="text-sm font-bold text-gray-500 uppercase tracking-wider mb-4">Delivery Log</h2>
      <div className="table-scroll">
        <table className="hp-table">
          <thead><tr><th>Order #</th><th>Client</th><th>Truck</th><th>Expected</th><th>Status</th></tr></thead>
          <tbody>
            {logs.slice(0,50).map(l => (
              <tr key={l.id}>
                <td className="font-bold text-[#07324C]">{l.order_number||'—'}</td>
                <td>{l.company_name||'—'}</td>
                <td>{l.truck_name||'—'}</td>
                <td>{l.expected_date||'—'}</td>
                <td className="capitalize">{l.status||'—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

// ═══════════════════════════════════════════════════════════════
// ADMIN PAGE
// ═══════════════════════════════════════════════════════════════
const AdminPage = ({ token }) => {
  const [tab, setTab] = useState('users');
  const [users, setUsers] = useState([]);
  const [zones, setZones] = useState([]);
  const [accConfig, setAccConfig] = useState(null);
  const [loading, setLoading] = useState(true);
  const [userModal, setUserModal] = useState(false);
  const [newUser, setNewUser] = useState({full_name:'',email:'',username:'',role:'floor_worker',pin:'',password:''});
  const [syncLog, setSyncLog] = useState([]);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const [u, z, acc, sl] = await Promise.all([
        api('/users', {}, token),
        api('/zones', {}, token),
        api('/accounting/config', {}, token),
        api('/accounting/sync-log?limit=10', {}, token),
      ]);
      setUsers(u);
      setZones(z);
      setAccConfig(acc);
      setSyncLog(sl);
    } catch (e) { toast(e.message,'error'); }
    setLoading(false);
  }, [token]);

  useEffect(() => { load(); }, [load]);

  const deactivateUser = async (id) => {
    try {
      await api(`/users/${id}`, {method:'DELETE'}, token);
      toast('User deactivated','info');
      load();
    } catch (e) { toast(e.message,'error'); }
  };

  const createUser = async () => {
    try {
      await api('/users', {method:'POST',body:JSON.stringify(newUser)}, token);
      toast('User created','success');
      setUserModal(false);
      setNewUser({full_name:'',email:'',username:'',role:'floor_worker',pin:'',password:''});
      load();
    } catch (e) { toast(e.message,'error'); }
  };

  const syncNow = async () => {
    try {
      const r = await api('/accounting/sync', {method:'POST'}, token);
      toast(r.message||'Sync triggered','success');
      load();
    } catch (e) { toast(e.message,'error'); }
  };

  const ROLES = ['executive','admin','office','planner','floor_worker','team_leader','qa','dispatch','ops_manager'];

  return (
    <div className="p-6 space-y-4">
      <h1 className="text-2xl font-black text-[#07324C]">Admin</h1>

      <div className="tab-bar">
        {['users','zones','accounting'].map(t => (
          <div key={t} className={`tab-item ${tab===t?'active':''}`} onClick={()=>setTab(t)} style={{textTransform:'capitalize'}}>{t}</div>
        ))}
      </div>

      {loading ? <div className="flex justify-center py-12"><Spinner /></div> : (
        <>
          {/* USERS */}
          {tab === 'users' && (
            <div className="hp-card">
              <div className="flex items-center justify-between mb-4">
                <h2 className="text-sm font-bold text-gray-500 uppercase tracking-wider">Users ({users.length})</h2>
                <button className="btn btn-primary btn-sm" onClick={()=>setUserModal(true)}>+ Add User</button>
              </div>
              <div className="table-scroll">
                <table className="hp-table">
                  <thead><tr><th>Name</th><th>Email / Username</th><th>Role</th><th>Zone</th><th>Active</th><th></th></tr></thead>
                  <tbody>
                    {users.map(u => (
                      <tr key={u.id}>
                        <td className="font-semibold">{u.full_name}</td>
                        <td className="text-xs text-gray-500">{u.email||u.username||'—'}</td>
                        <td><span className="badge badge-C" style={{textTransform:'capitalize'}}>{u.role?.replace('_',' ')}</span></td>
                        <td>{u.default_zone_id||'—'}</td>
                        <td>{u.is_active ? <span className="text-green-600 font-bold text-xs">Active</span> : <span className="text-gray-400 text-xs">Inactive</span>}</td>
                        <td>
                          {u.is_active && (
                            <button className="btn btn-danger btn-sm" onClick={()=>deactivateUser(u.id)}>Deactivate</button>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {userModal && (
                <div className="overlay" onClick={e=>e.target===e.currentTarget&&setUserModal(false)}>
                  <div className="modal">
                    <h3 className="font-black text-[#07324C] text-lg mb-4">Add New User</h3>
                    <div className="space-y-3">
                      {[['full_name','Full Name','text'],['email','Email','email'],['username','Username (floor)','text'],['password','Password','password'],['pin','PIN (floor)','text']].map(([field,label,type]) => (
                        <div key={field}>
                          <label className="hp-label">{label}</label>
                          <input type={type} className="hp-input" value={newUser[field]} onChange={e=>setNewUser(p=>({...p,[field]:e.target.value}))} />
                        </div>
                      ))}
                      <div>
                        <label className="hp-label">Role</label>
                        <select className="hp-input" value={newUser.role} onChange={e=>setNewUser(p=>({...p,role:e.target.value}))}>
                          {ROLES.map(r=><option key={r} value={r}>{r.replace('_',' ')}</option>)}
                        </select>
                      </div>
                    </div>
                    <div className="flex gap-3 mt-6">
                      <button className="btn btn-primary flex-1 justify-center" onClick={createUser}>Create User</button>
                      <button className="btn btn-secondary" onClick={()=>setUserModal(false)}>Cancel</button>
                    </div>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* ZONES */}
          {tab === 'zones' && (
            <div className="space-y-4">
              {zones.map(z => (
                <div key={z.id} className="hp-card">
                  <div className="flex items-center justify-between mb-3">
                    <div>
                      <h3 className="font-black text-[#07324C] text-lg">{z.name}</h3>
                      <div className="text-xs text-gray-400 uppercase tracking-wider mt-0.5">Code: {z.code} · Metric: {z.capacity_metric}</div>
                    </div>
                    <span className="badge badge-F">Active</span>
                  </div>
                  <div className="mt-2">
                    <div className="text-xs font-bold text-gray-400 uppercase tracking-wider mb-2">Stations ({z.stations?.length||0})</div>
                    <div className="flex flex-wrap gap-2">
                      {z.stations?.map(s => (
                        <div key={s.id} className="px-3 py-1 bg-gray-100 rounded-full text-xs font-medium text-gray-600">{s.name} <span className="text-gray-400">({s.station_type})</span></div>
                      ))}
                      {!z.stations?.length && <span className="text-xs text-gray-400">No stations configured</span>}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}

          {/* ACCOUNTING */}
          {tab === 'accounting' && (
            <div className="hp-card">
              <h2 className="text-sm font-bold text-gray-500 uppercase tracking-wider mb-4">Accounting Integration</h2>
              {accConfig && (
                <div className="space-y-3 mb-6">
                  <div className="flex items-center justify-between p-4 bg-gray-50 rounded-lg">
                    <div>
                      <div className="font-bold text-[#07324C]">Provider: {accConfig.provider || 'Mock'}</div>
                      <div className="text-xs text-gray-500 mt-1">Last sync: {accConfig.last_sync_at ? new Date(accConfig.last_sync_at).toLocaleString() : 'Never'}</div>
                    </div>
                    <div className={`px-3 py-1 rounded-full text-xs font-bold ${accConfig.is_connected ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-500'}`}>
                      {accConfig.is_connected ? 'Connected' : 'Not Connected'}
                    </div>
                  </div>
                  <button className="btn btn-primary btn-lg w-full justify-center" onClick={syncNow}>
                    <Icon name="RefreshCw" size={16}/> Sync Now
                  </button>
                </div>
              )}
              <h3 className="text-sm font-bold text-gray-500 uppercase tracking-wider mb-3">Sync History</h3>
              {syncLog.length === 0 ? <Empty msg="No sync history" /> : (
                <div className="table-scroll">
                  <table className="hp-table">
                    <thead><tr><th>Direction</th><th>Entity</th><th>Status</th><th>Details</th><th>Time</th></tr></thead>
                    <tbody>
                      {syncLog.map(l => (
                        <tr key={l.id}>
                          <td className="capitalize">{l.direction}</td>
                          <td>{l.entity_type}</td>
                          <td><span className={`font-bold text-xs ${l.status==='success'?'text-green-600':'text-red-600'}`}>{l.status}</span></td>
                          <td className="text-xs text-gray-500">{l.details}</td>
                          <td className="text-xs text-gray-500">{l.synced_at?.split('T')[0]}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}
        </>
      )}
    </div>
  );
};

// ═══════════════════════════════════════════════════════════════
// MAIN APP
// ═══════════════════════════════════════════════════════════════
const App = () => {
  const [user, setUser] = useState(null);
  const [token, setToken] = useState(null);
  const [page, setPage] = useState('dashboard');
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);

  const handleLogin = (u, t) => {
    if (!u || !u.role) { console.error('Invalid user object:', u); return; }
    setUser(u);
    setToken(t);
    // Redirect floor workers to production, prod managers to station allocation
    if (u.role === 'floor_worker' || u.role === 'team_leader') {
      setPage('floor');
      setSidebarCollapsed(true);
    } else if (u.role === 'production_manager') {
      setPage('allocation');
    } else {
      setPage('dashboard');
    }
  };

  const handleLogout = () => { setUser(null); setToken(null); setPage('dashboard'); };

  const handleNav = (p) => {
    setPage(p);
    if (p === 'floor') setSidebarCollapsed(true);
    else setSidebarCollapsed(false);
  };

  if (!user) return <LoginPage onLogin={handleLogin} />;

  const renderPage = () => {
    switch(page) {
      case 'dashboard': return <Dashboard token={token} />;
      case 'office': return <OfficeDashboard token={token} />;
      case 'planning': return <PlanningBoard token={token} />;
      case 'allocation': return <ProductionManager token={token} />;
      case 'floor': return <FloorTablet token={token} user={user} />;
      case 'qa': return <QADashboard token={token} />;
      case 'dispatch': return <DispatchDashboard token={token} />;
      case 'admin': return <AdminPage token={token} />;
      case 'ops': return <Dashboard token={token} isOps={true} />;
      default: return <Dashboard token={token} />;
    }
  };

  return (
    <div style={{display:'flex',height:'100vh',overflow:'hidden'}}>
      <Sidebar page={page} onNav={handleNav} user={user} onLogout={handleLogout} collapsed={sidebarCollapsed} />
      <main style={{flex:1,overflow:'auto',background:'#F5F7FA'}}>
        {renderPage()}
      </main>
    </div>
  );
};

// Error boundary to catch rendering crashes
class ErrorBoundary extends React.Component {
  constructor(props) { super(props); this.state = { error: null, info: null }; }
  static getDerivedStateFromError(error) { return { error }; }
  componentDidCatch(error, info) { this.setState({ info }); console.error('React crash:', error, info); }
  render() {
    if (this.state.error) {
      return React.createElement('div', { style: { padding: '40px', fontFamily: 'monospace' } },
        React.createElement('h1', { style: { color: '#ED1C24' } }, 'Application Error'),
        React.createElement('p', null, this.state.error.toString()),
        React.createElement('pre', { style: { background: '#f5f5f5', padding: '20px', overflow: 'auto', fontSize: '12px' } },
          this.state.info?.componentStack || 'No stack trace')
      );
    }
    return this.props.children;
  }
}

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(<ErrorBoundary><App /></ErrorBoundary>);
</script>
</body>
</html>
