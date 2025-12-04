import asyncio
import subprocess
import os
import psutil
import time
import logging
from pathlib import Path
from config import BotConfig

logger = logging.getLogger(__name__)

class ComfyProcessManager:
    def __init__(self, config: BotConfig):
        self.config = config
        # Load these from env or config, falling back to the discovered paths if not set
        self.python_exe = os.getenv("COMFYUI_PYTHON_EXE", r"J:\ComfyUI\.venv\Scripts\python.exe")
        self.main_script = os.getenv("COMFYUI_MAIN_SCRIPT", r"C:\Users\vital\AppData\Local\Programs\@comfyorgcomfyui-electron\resources\ComfyUI\main.py")
        # Default args from the discovered process
        default_args = (
            r"--user-directory J:\ComfyUI\user "
            r"--input-directory J:\ComfyUI\input "
            r"--output-directory J:\ComfyUI\output "
            r"--front-end-root C:\Users\vital\AppData\Local\Programs\@comfyorgcomfyui-electron\resources\ComfyUI\web_custom_versions\desktop_app "
            r"--base-directory J:\ComfyUI "
            r"--extra-model-paths-config C:\Users\vital\AppData\Roaming\ComfyUI\extra_models_config.yaml "
            r"--log-stdout --listen 127.0.0.1 --port 8000 "
            r"--force-fp16 --fp8_e4m3fn-unet --fp16-vae --preview-method taesd --preview-size 1280 --lowvram"
        )
        self.args = os.getenv("COMFYUI_ARGS", default_args)
        self.process = None

    def is_running(self) -> bool:
        """Check if ComfyUI is running on the configured port."""
        # Simple check: is there a python process with main.py and the port?
        target_port = "8000" # We enforce 8000 as base
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = proc.info['cmdline']
                if cmdline and 'python' in proc.info['name'].lower():
                    # Check if it's running ComfyUI main.py
                    if any('main.py' in arg for arg in cmdline):
                        # Check port
                        if f"--port {target_port}" in " ".join(cmdline) or (target_port == "8188" and "--port" not in cmdline):
                             return True
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        return False

    def kill_all_instances(self):
        """Kill all ComfyUI instances to ensure a clean slate."""
        logger.info("Stopping all ComfyUI instances...")
        killed_count = 0
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = proc.info['cmdline']
                if cmdline and 'python' in proc.info['name'].lower():
                    # Identify ComfyUI processes
                    if any('ComfyUI' in arg and 'main.py' in arg for arg in cmdline):
                        logger.info(f"Killing process {proc.info['pid']}")
                        proc.kill()
                        killed_count += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        
        if killed_count > 0:
            logger.info(f"Killed {killed_count} ComfyUI processes.")
            time.sleep(2) # Wait for ports to free up

    def start(self):
        """Start ComfyUI."""
        if not os.path.exists(self.python_exe):
            logger.error(f"Python executable not found at {self.python_exe}")
            return
        
        if not os.path.exists(self.main_script):
            logger.error(f"ComfyUI main script not found at {self.main_script}")
            return

        cmd = [self.python_exe, self.main_script] + self.args.split()
        
        logger.info(f"Starting ComfyUI with command: {' '.join(cmd)}")
        
        # Start as a subprocess
        # We use Popen to let it run in background
        self.process = subprocess.Popen(
            cmd,
            cwd=os.path.dirname(self.main_script),
            stdout=subprocess.DEVNULL, # Redirect output or keep it?
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NEW_CONSOLE # Open in new window or hidden? User said "auto-start", maybe hidden is better or minimized.
            # But usually users want to see the console. 
            # CREATE_NEW_CONSOLE might be annoying if it pops up.
            # Let's try standard Popen.
        )
        
        logger.info(f"ComfyUI started with PID {self.process.pid}")
        
        # Wait for it to be ready?
        # We can just return and let the bot retry connection.

    async def restart(self):
        self.kill_all_instances()
        self.start()

