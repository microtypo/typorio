import click
import asyncio

from pynput import keyboard, mouse
from typorio.core import constants
from typorio.core.worker import Worker


@click.command(help="Start Typorio")
@click.option("--env", type=str, default="prod")
@click.option("--profile", type=str, default="typo")
@click.option("--quiet", is_flag=True, default=False)
@click.option("--dry-run", is_flag=True, default=False)
@click.option("--shuffle", is_flag=True, default=True)
@click.option("--max-rows", type=int, default=100)
@click.option("--push-interval", type=int, default=60 * 10)
def start(
        env: str,
        profile: str,
        quiet: bool,
        dry_run: bool,
        shuffle: bool,
        max_rows: int,
        push_interval: int,
):
    worker = Worker(
        env=env,
        profile=profile,
        verbose=not quiet,
        dry_run=dry_run,
        shuffle=shuffle,
        max_rows=max_rows,
        push_interval=push_interval,
    )

    def on_click(x, y, button, pressed):
        if pressed:
            worker.write_data(event=constants.MOUSE, key=button.name, meta={"x": x, "y": y})

    mouse_listener = mouse.Listener(on_click=on_click)

    def on_press(key):
        try:
            worker.write_data(
                key=key.char,
                event=constants.KEYBOARD,
                meta={"vk": getattr(key, "vk", None)}
            )
        except AttributeError:
            worker.write_data(
                key=key.name,
                event=constants.KEYBOARD,
                meta={"vk": getattr(key, "vk", None)}
            )

    keyboard_listener = keyboard.Listener(on_press=on_press)

    hotkeys = {
        "<ctrl>+<alt>+<shift>+p": lambda: worker.push_data(),
        # "<ctrl>+<alt>+<shift>+m": lambda: worker.move_data(),
    }

    global_listener = keyboard.GlobalHotKeys(hotkeys)

    async def listen():
        keyboard_listener.start()
        mouse_listener.start()
        global_listener.start()

    loop = asyncio.new_event_loop()
    loop.create_task(worker.sync_data())
    loop.create_task(listen())
    loop.run_forever()
