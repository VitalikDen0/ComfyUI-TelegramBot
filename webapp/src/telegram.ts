export function getTelegramWebApp() {
  return (window as any)?.Telegram?.WebApp;
}

export function initTelegramUi(): void {
  const tg = getTelegramWebApp();
  if (!tg) return;
  try {
    tg.ready();
    tg.expand();
    tg.setBackgroundColor("#0b1220");
    tg.setHeaderColor("secondary_bg_color");
  } catch (err) {
    console.debug("Telegram WebApp init failed", err);
  }
}

export function showAlert(message: string): void {
  const tg = getTelegramWebApp();
  if (tg?.showAlert) {
    tg.showAlert(message);
  } else {
    alert(message);
  }
}
