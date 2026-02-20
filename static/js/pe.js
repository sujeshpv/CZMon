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