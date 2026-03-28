// 注意：等 Render 架好後，把這裡換成你真實的 Render 網址
const API_URL = 'https://你的-render-app名稱.onrender.com/stocks';

async function runStrategy() {
  const tbody = document.getElementById('resultBody');
  const status = document.getElementById('status');
  
  status.textContent = "資料讀取中，Render 免費版如果很久沒用，大約需要等 30~50 秒喚醒...";
  tbody.innerHTML = `<tr><td colspan="7">讀取中...</td></tr>`;

  try {
    const response = await fetch(API_URL);
    if (!response.ok) throw new Error('伺服器回應錯誤');
    
    const stocks = await response.json();
    
    if (stocks.length === 0) {
      tbody.innerHTML = `<tr><td colspan="7">今日無符合條件的股票</td></tr>`;
      status.textContent = "讀取完成";
      return;
    }

    tbody.innerHTML = stocks.map(stock => `
      <tr>
        <td>${stock.code}</td>
        <td>${stock.name}</td>
        <td>${stock.close}</td>
        <td>${stock.ma5}</td>
        <td>${stock.ma20}</td>
        <td>${stock.ma60}</td>
        <td>${stock.volume}</td>
      </tr>
    `).join('');
    
    status.textContent = `讀取完成，共找到 ${stocks.length} 檔`;
    
  } catch (error) {
    status.textContent = `發生錯誤：${error.message}`;
    tbody.innerHTML = `<tr><td colspan="7">讀取失敗</td></tr>`;
  }
}

document.getElementById('runBtn').addEventListener('click', runStrategy);
