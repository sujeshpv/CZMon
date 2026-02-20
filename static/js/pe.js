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
