(function(){
  const toggle = document.getElementById('sidebar-toggle');
  const sidebar = document.getElementById('sidebar');
  const STORAGE_KEY = 'cz_sidebar_collapsed';
  if(!toggle || !sidebar) return;

  function setState(collapsed){
    if(collapsed){
      sidebar.classList.add('collapsed');
      toggle.textContent = '⟩';
    } else {
      sidebar.classList.remove('collapsed');
      toggle.textContent = '⟨';
    }
    try{ localStorage.setItem(STORAGE_KEY, collapsed? '1':'0'); }catch(e){}
  }

  // initialize from storage
  try{
    const val = localStorage.getItem(STORAGE_KEY);
    if(val === '1') setState(true);
  }catch(e){}

  toggle.addEventListener('click', function(){
    setState(sidebar.classList.contains('collapsed') ? false : true);
  });
})();
// Handle external navigation links
document.querySelectorAll('a.nav-link').forEach(link => {
  link.addEventListener('click', function(e){
    e.preventDefault();
    const url = this.dataset.url;
    if(!url) return;
    
    const main = document.querySelector('main.main');
    main.innerHTML = '<iframe src="' + url + '" style="width:100%; height:100%; border:none;"></iframe>';
    
    // Update active state
    document.querySelectorAll('a').forEach(a => a.classList.remove('active'));
    this.classList.add('active');
  });
});

// Confirmation for delete buttons in settings page
(function(){
  // find all forms with a submit button that has a name starting with delete_
  document.querySelectorAll('form').forEach(form => {
    const submit = form.querySelector('button[type="submit"][name^="delete_"]');
    if(!submit) return;
    submit.addEventListener('click', function(e){
      const ip = form.querySelector('input[name="ip"]')?.value || '';
      const ok = confirm(ip ? `Delete ${ip}? This cannot be undone.` : 'Delete selected item?');
      if(!ok){
        e.preventDefault();
      }
    });
  });
})();

// Client-side validation for Add PC / Add PE forms
(function(){
  function showClientMessage(level, text){
    let container = document.querySelector('.messages');
    if(!container){
      const main = document.querySelector('main.main');
      container = document.createElement('ul');
      container.className = 'messages';
      main.insertBefore(container, main.firstChild);
    }
    container.innerHTML = `<li class="${level}">${text}</li>`;
    // auto-clear after 4s
    setTimeout(()=>{ if(container) container.remove(); }, 4000);
  }

  function isDuplicate(value, listId){
    const list = document.getElementById(listId);
    if(!list) return false;
    return Array.from(list.querySelectorAll('li')).some(li => {
      const ip = (li.dataset && li.dataset.ip) ? li.dataset.ip.trim() : '';
      if(ip) return ip === value;
      // fallback: check if text ends with the value (handles legacy plain text entries)
      return li.textContent.trim().endsWith(value);
    });
  }

  function isDuplicatePcByIpOrName(ipVal, nameVal){
    const table = document.getElementById('pcs-list');
    if(!table) return { ip: false, name: false };
    const rows = table.querySelectorAll('tbody tr[data-ip]');
    let dupIp = false, dupName = false;
    rows.forEach(tr => {
      if((tr.dataset.ip || '').trim() === ipVal) dupIp = true;
      if((tr.dataset.name || '').trim() === nameVal) dupName = true;
    });
    return { ip: dupIp, name: dupName };
  }

  // handle add forms
  const addPcForm = document.querySelector('form button[name="add_pc"]')?.closest('form');
  const addPeForm = document.querySelector('form button[name="add_pe"]')?.closest('form');

  if(addPcForm){
    addPcForm.addEventListener('submit', function(e){
      const nameInput = addPcForm.querySelector('input[name="pc_name"]');
      const ipInput = addPcForm.querySelector('input[name="pc_ip"]');
      const nameVal = (nameInput?.value || '').trim();
      const ipVal = (ipInput?.value || '').trim();
      if(!nameVal){
        e.preventDefault();
        showClientMessage('error', 'PC Name is required and cannot be blank');
        return;
      }
      if(!ipVal){
        e.preventDefault();
        showClientMessage('error', 'Please provide PC Virtual IP/FQDN');
        return;
      }
      const dup = isDuplicatePcByIpOrName(ipVal, nameVal);
      if(dup.ip){
        e.preventDefault();
        showClientMessage('info', `PC already configured: ${ipVal}`);
        return;
      }
      if(dup.name){
        e.preventDefault();
        showClientMessage('error', `PC Name must be unique: ${nameVal}`);
        return;
      }
      // allow submit
    });
  }

  if(addPeForm){
    addPeForm.addEventListener('submit', function(e){
      const ipInput = addPeForm.querySelector('input[name="pe_ip"]');
      const nameInput = addPeForm.querySelector('input[name="pe_name"]');
      const ipVal = ipInput?.value.trim() || '';
      const nameVal = nameInput?.value.trim() || '';
      if(!ipVal){
        e.preventDefault();
        showClientMessage('error', 'Please provide PE Virtual IP/FQDN');
        return;
      }
      if(!nameVal){
        e.preventDefault();
        showClientMessage('error', 'PE Name is required');
        return;
      }
      // Check duplicate IP
      if(isDuplicate(ipVal, 'pes-list')){
        e.preventDefault();
        showClientMessage('info', `PE already configured: ${ipVal}`);
        return;
      }
      // Check duplicate Name
      const pesList = document.getElementById('pes-list');
      if(pesList && Array.from(pesList.querySelectorAll('li')).some(li => (li.dataset && li.dataset.name && li.dataset.name.trim() === nameVal))){
        e.preventDefault();
        showClientMessage('info', `PE Name already exists: ${nameVal}`);
        return;
      }
      // allow submit
    });
  }
})();