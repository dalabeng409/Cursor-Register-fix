import os
import csv
import copy
import argparse
import concurrent.futures
import sys
import hydra
from faker import Faker
from datetime import datetime
from omegaconf import OmegaConf, DictConfig
from DrissionPage import ChromiumOptions, Chromium
import time # <--- 1. 导入 time 模块
import traceback # <--- 2. 导入 traceback 模块

# 设置控制台输出编码为UTF-8，避免中文字符编码问题
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# 假设这些导入是正确的
from temp_mails import Tempmail_io, Guerillamail_com
from helper.cursor_register import CursorRegister
from helper.email import *

# Parameters for debugging purpose
hide_account_info = os.getenv('HIDE_ACCOUNT_INFO', 'false').lower() == 'true'
enable_headless = os.getenv('ENABLE_HEADLESS', 'false').lower() == 'true'
enable_browser_log = os.getenv('ENABLE_BROWSER_LOG', 'true').lower() == 'true' or not enable_headless

# --- 修改后的核心注册函数 ---
def register_cursor_core(register_config, options, index): # <--- 3. 添加 index 参数
    """处理单个账户注册的核心逻辑"""
    pid = os.getpid()
    log_prefix = f"[Register][{pid}][{index}]" # <--- 4. 创建带索引的日志前缀

    print(f"{log_prefix} Starting task for account index {index}")

    # ==================================================
    # ========== 5. 在此处添加延迟 ==========
    # ==================================================
    # !!! 这是关键修改：增加延迟以避免 IMAP 速率限制 !!!
    # 你可以根据需要调整这个秒数，如果仍然失败，请尝试增加到 45, 60 或更长
    delay_seconds = 30 # <--- 初始建议增加到 30 秒
    print(f"{log_prefix} Waiting for {delay_seconds} seconds before processing account {index}...")
    time.sleep(delay_seconds)
    print(f"{log_prefix} Wait finished for account {index}.")
    # ==================================================

    browser = None
    email_server = None
    status = False
    token = None
    email_address = "[Unknown]" # 初始化，以便在出错时也能记录

    try:
        # --- 6. 更健壮的浏览器启动 ---
        try:
            print(f"{log_prefix} Launching browser...")
            # 将 email_address 提取到这里，以便在浏览器错误时也能记录
            if register_config.email_server.name == "imap_email_server":
                 email_address = register_config.email_server.email_address
            # 注意：如果这里失败，可能是环境问题或 DrissionPage/驱动问题
            browser = Chromium(options)
            print(f"{log_prefix} Browser launched successfully.")
        except Exception as browser_e:
            print(f"{log_prefix} !!! Failed to launch browser for account {index} ({email_address}): {browser_e}")
            # 打印详细错误信息
            print(traceback.format_exc())
            # 浏览器启动失败，无法继续此任务
            return None

        # --- 7. 设置邮件服务器并处理 IMAP 连接错误 ---
        print(f"{log_prefix} Setting up email server...")
        if register_config.email_server.name == "temp_email_server":
            try:
                 email_server = eval(register_config.temp_email_server.name)(browser)
                 email_address = email_server.get_email_address() # 获取临时邮箱地址
                 print(f"{log_prefix} Using Temp Email: {email_address}")
            except Exception as temp_email_e:
                 print(f"{log_prefix} !!! Failed to set up temporary email server: {temp_email_e}")
                 print(traceback.format_exc())
                 return None # 无法设置邮箱服务器
        elif register_config.email_server.name == "imap_email_server":
            # email_address 已在前面获取
            imap_config = register_config.email_server.imap_config
            imap_server_host = imap_config.imap_server
            imap_port = imap_config.imap_port
            imap_username = imap_config.username
            imap_password = imap_config.password

            # --- 8. 明确捕获 IMAP 初始化/登录错误 ---
            try:
                print(f"{log_prefix} Connecting to IMAP server {imap_server_host} for {email_address}...")
                email_server = Imap(imap_server_host, imap_port, imap_username, imap_password, email_to=email_address)
                # 如果 Imap() 初始化或内部的 login() 失败，会抛出异常
                print(f"{log_prefix} IMAP connection/login successful for {email_address}.")
            except Exception as imap_e:
                print(f"{log_prefix} !!! Failed to connect or login to IMAP server for {email_address}: {imap_e}")
                print(traceback.format_exc())
                # IMAP 失败，返回 None，将在 finally 中关闭浏览器
                return None
        else:
            print(f"{log_prefix} !!! Unknown email server type: {register_config.email_server.name}")
            return None

        # --- 9. 执行注册/登录流程并处理其错误 ---
        try:
            print(f"{log_prefix} Initializing CursorRegister for {email_address}...")
            register = CursorRegister(browser, email_server)
            print(f"{log_prefix} Attempting sign in/up for {email_address}...")
            # 注意：原始代码是 sign_in，如果需要注册新号，可能要改成 sign_up
            # 这里假设 register.sign_in 内部会处理获取验证码等逻辑
            tab_signin, status = register.sign_in(email_address)
            # tab_signup, status = register.sign_up(email_address) # 注册新号选项

            if not status:
                 # 这里可能包含了获取验证码失败（Fail to get code from email）、Turnstile失败等
                 print(f"{log_prefix} Sign in/up process failed for {email_address}. Check CursorRegister logs or previous steps.")
                 return None # 注册/登录流程失败

            print(f"{log_prefix} Sign in/up process successful, getting cookie for {email_address}...")
            token = register.get_cursor_cookie(tab_signin)

            if token is None:
                 print(f"{log_prefix} Failed to obtain Cursor cookie/token after successful sign in/up for {email_address}.")
                 return None # 无法获取 token

            print(f"{log_prefix} Successfully obtained token for {email_address}.")

        except Exception as register_e:
             print(f"{log_prefix} !!! An error occurred during the Cursor registration/login process for {email_address}: {register_e}")
             print(traceback.format_exc())
             return None # 注册流程中出现意外错误

        # --- 10. 可选的低余额账户处理 (保持原逻辑，增加错误捕获) ---
        if token and register_config.email_server.name == "imap_email_server":
            try:
                user_id_parts = token.split("%3A%3A")
                if len(user_id_parts) > 0:
                    user_id = user_id_parts[0]
                    # 使用 .get() 安全访问配置，避免 Key Error
                    delete_low_balance = register_config.get('delete_low_balance_account', False)
                    if delete_low_balance:
                        threshold = register_config.get('delete_low_balance_account_threshold', 0)
                        print(f"{log_prefix} Checking account balance for {email_address} (User ID: {user_id})...")
                        usage = register.get_usage(user_id)
                        if usage and "gpt-4" in usage:
                            balance = usage["gpt-4"]["maxRequestUsage"] - usage["gpt-4"]["numRequests"]
                            print(f"{log_prefix} Current balance: {balance}")
                            if balance <= threshold:
                                print(f"{log_prefix} [Low Balance] Balance ({balance}) <= threshold ({threshold}), attempting delete and re-register.")
                                if register.delete_account():
                                    print(f"{log_prefix} [Low Balance] Account deleted, re-registering (sign up)...")
                                    tab_signup, status_signup = register.sign_up(email_address) # 尝试注册
                                    if status_signup:
                                        new_token = register.get_cursor_cookie(tab_signup)
                                        if new_token:
                                            print(f"{log_prefix} [Low Balance] Re-registration successful.")
                                            token = new_token # 更新 token
                                        else:
                                            print(f"{log_prefix} [Low Balance] Re-registration failed to get token.")
                                            status = False # 标记最终状态为失败
                                    else:
                                        print(f"{log_prefix} [Low Balance] Re-registration sign up failed.")
                                        status = False # 标记最终状态为失败
                                else:
                                    print(f"{log_prefix} [Low Balance] Failed to delete account.")
                        else:
                            print(f"{log_prefix} Could not get valid usage info for balance check.")
                else:
                    # 如果 delete_low_balance_account 未启用，则不执行检查
                    pass # 或者打印一条信息说明跳过余额检查
            except Exception as balance_e:
                print(f"{log_prefix} Warning: Error during optional balance check/delete operation: {balance_e}")
                # 不因此操作失败而中断整个注册

        # --- 11. 构造成功返回值 ---
        if status and token: # 确保流程成功并且获取到了 token
            if not hide_account_info:
                print(f"{log_prefix} Cursor Email: {email_address}")
                print(f"{log_prefix} Cursor Token: {token}")
            # 返回包含状态的字典
            return {
                "username": email_address,
                "token": token,
                "status": "success" # 明确成功状态
            }
        else:
             # 如果到这里 status 为 False 或 token 为 None
             print(f"{log_prefix} Registration ultimately failed for {email_address} (Final Status: {status}, Token Found: {token is not None}).")
             return None # 返回 None 表示失败

    except Exception as core_e:
        # 捕获整个函数执行过程中的任何其他意外错误
        print(f"{log_prefix} !!! An unexpected error occurred in register_cursor_core for account {index} ({email_address}): {core_e}")
        print(traceback.format_exc())
        return None # 确保返回 None

    finally:
        # --- 12. 确保浏览器被关闭 ---
        if browser:
            print(f"{log_prefix} Attempting to quit browser for account {index}...")
            try:
                browser.quit(force=True, del_data=True)
                print(f"{log_prefix} Browser quit.")
            except Exception as quit_e:
                print(f"{log_prefix} Warning: Error quitting browser: {quit_e}")

# --- 修改后的协调函数 ---
def register_cursor(register_config):

    options = ChromiumOptions()
    options.auto_port()
    options.new_env()
    # Use turnstilePatch from https://github.com/TheFalloutOf76/CDP-bug-MouseEvent-.screenX-.screenY-patcher
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        turnstile_patch_path = os.path.abspath(os.path.join(current_dir, "turnstilePatch"))
        if os.path.exists(turnstile_patch_path):
             print(f"[Register] Adding Turnstile Patch from: {turnstile_patch_path}")
             options.add_extension(turnstile_patch_path)
        else:
             print(f"[Register] Warning: Turnstile Patch directory not found at {turnstile_patch_path}.")
    except Exception as patch_e:
         print(f"[Register] Warning: Error setting up Turnstile Patch: {patch_e}")

    if enable_headless:
        print("[Register] Headless mode enabled, configuring...")
        try:
            import platform
            platform_system = platform.system()
            platformIdentifier = ""
            if platform_system == "Linux": platformIdentifier = "X11; Linux x86_64"
            elif platform_system == "Darwin": platformIdentifier = "Macintosh; Intel Mac OS X 10_15_7"
            elif platform_system == "Windows": platformIdentifier = "Windows NT 10.0; Win64; x64"
            else: platformIdentifier = "Unknown"
            chrome_version = "130.0.0.0" # Consider making this configurable
            user_agent = f"Mozilla/5.0 ({platformIdentifier}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_version} Safari/537.36"
            print(f"[Register] Setting User-Agent: {user_agent}")
            options.set_user_agent(user_agent)
            options.headless()
            print("[Register] Headless mode configured.")
        except Exception as ua_e:
             print(f"[Register] Warning: Error configuring headless: {ua_e}")

    number = register_config.number
    max_workers = register_config.max_workers # 应保持为 1
    print(f"[Register] Start to register {number} accounts in {max_workers} threads")

    results = []
    successful_accounts_count = 0 # <--- 13. 初始化成功计数器
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for idx in range(number):
            register_config_thread = copy.deepcopy(register_config)
            use_custom_address = register_config.email_server.use_custom_address

            if use_custom_address and register_config.email_server.name == "imap_email_server":
                if hasattr(register_config.email_server, 'custom_email_addresses') and idx < len(register_config.email_server.custom_email_addresses):
                    email_config = register_config.email_server.custom_email_addresses[idx]
                    # 确保传递的是 OmegaConf 对象或字典
                    if isinstance(email_config, DictConfig):
                         register_config_thread.email_server.email_address = email_config.get('email')
                         register_config_thread.email_server.imap_config = email_config
                    elif isinstance(email_config, dict): # 如果是从 JSON 加载的字典
                         register_config_thread.email_server.email_address = email_config.get('email')
                         register_config_thread.email_server.imap_config = OmegaConf.create(email_config) # 转为 OmegaConf
                    else:
                         print(f"[Register] Warning: Invalid email configuration type for index {idx}. Skipping.")
                         continue

                    if not register_config_thread.email_server.email_address:
                         print(f"[Register] Warning: Email address missing in configuration for index {idx}. Skipping.")
                         continue
                else:
                    print(f"[Register] Warning: No valid email configuration found for index {idx}. Skipping.")
                    continue

            options_thread = copy.deepcopy(options)
            # 提交任务，传递索引
            futures.append(executor.submit(register_cursor_core, register_config_thread, options_thread, idx)) # <--- 14. 传递 idx

        # 处理完成的任务
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                # --- 15. 检查结果是否有效 ---
                if result and isinstance(result, dict) and result.get("status") == "success" and result.get("token"):
                    results.append(result)
                    successful_accounts_count += 1 # 成功计数
                    print(f"[Register] Task for account {result.get('username')} completed successfully.")
                else:
                    # 失败信息应在 core 函数内部打印
                    print(f"[Register] A registration task failed or returned invalid result.")
            except Exception as future_e:
                 print(f"[Register] !!! An exception occurred while processing future result:")
                 print(traceback.format_exc())

    # --- 16. 准确写入文件和报告 ---
    # 使用实际成功的计数
    print(f"[Register] Finished processing all tasks. Total {successful_accounts_count} accounts registered successfully (out of {number} attempted).")

    # 只有当有成功结果时才写入文件
    if successful_accounts_count > 0:
        valid_results = [r for r in results if r and r.get("token")] # 再次过滤确保 token 存在
        if valid_results: # 可能 result 存在但 token 意外丢失
            formatted_date = datetime.now().strftime("%Y-%m-%d")
            output_csv_path = f"./output_{formatted_date}.csv"
            token_csv_path = f"./token_{formatted_date}.csv"

            # 确保字段名基于第一个有效结果
            fieldnames = valid_results[0].keys()

            print(f"[Register] Writing {len(valid_results)} successful account(s) info to {output_csv_path}...")
            file_exists = os.path.isfile(output_csv_path)
            try:
                 with open(output_csv_path, 'a', newline='', encoding='utf-8') as file:
                     writer = csv.DictWriter(file, fieldnames=fieldnames)
                     if not file_exists or os.path.getsize(output_csv_path) == 0:
                         writer.writeheader()
                     writer.writerows(valid_results)
                 print(f"[Register] Account info written.")
            except IOError as csv_e:
                 print(f"[Register] !!! Error writing to output CSV file {output_csv_path}: {csv_e}")


            tokens_to_write = [{'token': row['token']} for row in valid_results]
            print(f"[Register] Writing {len(tokens_to_write)} token(s) to {token_csv_path}...")
            try:
                 with open(token_csv_path, 'a', newline='', encoding='utf-8') as file:
                     writer = csv.DictWriter(file, fieldnames=['token'])
                     # token 文件通常不需要 header
                     writer.writerows(tokens_to_write)
                 print(f"[Register] Tokens written.")
            except IOError as token_csv_e:
                 print(f"[Register] !!! Error writing to token CSV file {token_csv_path}: {token_csv_e}")

        else:
             print("[Register] No valid results with tokens found to write, although count > 0. Check results list.")
             return [] # 返回空列表
    else:
         print("[Register] No accounts were registered successfully in this run.")
         return [] # 没有成功，返回空列表

    # 返回实际有效的账户信息列表
    return valid_results


@hydra.main(config_path="config", config_name="config", version_base=None)
def main(config: DictConfig):
    print("[Main] Starting main function...")
    OmegaConf.set_struct(config, False)

    # --- 环境变量处理 (与之前修改一致) ---
    use_config_file_str = os.getenv('REGISTER_USE_CONFIG_FILE', 'true')
    use_config_file = use_config_file_str.lower() != 'false'

    if not use_config_file:
        email_configs_str = os.getenv('REGISTER_EMAIL_CONFIGS', '[]')
        print("[Main] Attempting to use email configurations from environment variable REGISTER_EMAIL_CONFIGS.")
        try:
            import json
            email_configs = json.loads(email_configs_str)
            if not isinstance(email_configs, list):
                raise ValueError('REGISTER_EMAIL_CONFIGS environment variable must be a valid JSON list')
            if hasattr(config, 'register') and hasattr(config.register, 'email_server'):
                 config.register.email_server.custom_email_addresses = OmegaConf.create(email_configs)
                 config.register.email_server.use_custom_address = True
                 config.register.email_server.name = "imap_email_server"
                 print(f'[Main] Successfully loaded {len(email_configs)} email configurations from environment variables.')
            else:
                 print("[Main] Warning: Config structure issue. Cannot apply env var configs.")
        except (json.JSONDecodeError, ValueError) as e:
            print(f'[Main] Error processing REGISTER_EMAIL_CONFIGS: {e}. Exiting.')
            return
    else:
        print('[Main] Using email configurations from config file (config/config.yaml).')

    # --- 配置验证和调整 (与之前修改一致) ---
    print("[Main] Validating and adjusting configuration...")
    if not hasattr(config, 'register') or not hasattr(config.register, 'email_server'):
         print("[Main] Error: Configuration section 'register.email_server' missing.")
         return

    email_server_name = config.register.email_server.get('name')
    use_custom_address = config.register.email_server.get('use_custom_address', False)

    if email_server_name not in ["temp_email_server", "imap_email_server"]:
         print(f"[Main] Error: Invalid email_server name '{email_server_name}'.")
         return
    if use_custom_address and email_server_name != "imap_email_server":
         print("[Main] Error: 'use_custom_address' only valid with 'imap_email_server'.")
         return

    if use_custom_address and email_server_name == "imap_email_server":
        custom_emails = config.register.email_server.get('custom_email_addresses')
        if isinstance(custom_emails, list):
            num_custom_emails = len(custom_emails)
            if num_custom_emails > 0:
                config.register.number = num_custom_emails
                print(f"[Main] register.number set to {config.register.number} based on custom_email_addresses.")
            else:
                print("[Main] Warning: 'custom_email_addresses' list is empty. Setting register.number to 0.")
                config.register.number = 0
        else:
            print("[Main] Error: 'custom_email_addresses' is missing or not a list.")
            return
    elif not use_custom_address:
         reg_num = config.register.get('number')
         if not isinstance(reg_num, int) or reg_num <= 0:
              print("[Main] Error: 'register.number' must be positive integer when not using custom emails.")
              return
         print(f"[Main] Registering {reg_num} accounts using {email_server_name}.")
    else:
         # Should not happen based on previous checks, but as a safeguard
         print("[Main] Error: Inconsistent configuration state.")
         return

    # 如果最终计算出的注册数量为0，则提前退出
    if config.register.get('number', 0) == 0:
         print("[Main] Number of accounts to register is 0. Exiting.")
         return

    # --- 执行注册 ---
    print("[Main] Calling register_cursor function...")
    account_infos = register_cursor(config.register) # 传递 register 子配置

    # --- 后续处理 (OneAPI - 与之前修改一致) ---
    if config.oneapi.get('enabled', False) and len(account_infos) > 0:
        print("[Main] OneAPI integration enabled, processing tokens...")
        try:
            from tokenManager.oneapi_manager import OneAPIManager
            oneapi_url = config.oneapi.get('url')
            oneapi_token = config.oneapi.get('token')
            oneapi_channel_url = config.oneapi.get('channel_url')

            if not all([oneapi_url, oneapi_token, oneapi_channel_url]):
                print("[Main] Warning: OneAPI config missing. Skipping.")
            else:
                print(f"[Main] Connecting to OneAPI at {oneapi_url}...")
                oneapi = OneAPIManager(oneapi_url, oneapi_token)
                tokens = list(set([row['token'] for row in account_infos if row.get('token')]))

                if tokens:
                     batch_size = min(10, len(tokens))
                     print(f"[Main] Adding {len(tokens)} tokens to OneAPI in batches of {batch_size}...")
                     for i in range(0, len(tokens), batch_size):
                         batch_tokens = tokens[i:i+batch_size]
                         print(f"[Main] Processing batch {i//batch_size + 1}...")
                         oneapi.batch_add_channel(batch_tokens, oneapi_channel_url)
                     print("[Main] Finished adding tokens to OneAPI.")
                else:
                     print("[Main] No valid tokens found for OneAPI.")
        except ImportError:
            print("[Main] Warning: OneAPIManager modules not found. Skipping OneAPI.")
        except Exception as oneapi_e:
             print(f"[Main] Error during OneAPI integration: {oneapi_e}")
             print(traceback.format_exc())
    elif config.oneapi.get('enabled', False):
        print("[Main] OneAPI enabled, but no accounts registered. Skipping.")

    print("[Main] Main function finished.")

if __name__ == "__main__":
    # 确保 Hydra 能找到配置文件目录
    # 通常 Hydra 会自动处理，但如果脚本不在仓库根目录运行，可能需要调整
    main()
