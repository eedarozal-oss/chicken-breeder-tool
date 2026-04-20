from __future__ import annotations

import json
from services.wallet_item_inventory import (
    build_wallet_inventory_name_lookup,
    normalize_item_name,
)

MAX_MASS_BREEDING_PAIRS = 10


def build_bookmarklet_payload_rows(
    planner_queue,
    script_mode="full",
    inventory_name_lookup=None,
    max_pairs=MAX_MASS_BREEDING_PAIRS,
):
    payload_rows = []

    script_mode = str(script_mode or "full").strip().lower()
    if script_mode not in {"full", "partial", "no_items"}:
        script_mode = "full"

    inventory_remaining = {}
    for item_name, item in (inventory_name_lookup or {}).items():
        canonical_name = normalize_item_name(item_name)
        if not canonical_name:
            continue

        balance_raw = item.get("balance")
        try:
            balance = int(str(balance_raw or "0"))
        except ValueError:
            balance = 0

        inventory_remaining[canonical_name] = balance

    try:
        max_pairs = int(max_pairs)
    except (TypeError, ValueError):
        max_pairs = MAX_MASS_BREEDING_PAIRS
    max_pairs = max(0, max_pairs)

    for row in list(planner_queue or [])[:max_pairs]:
        left = row.get("left") or {}
        right = row.get("right") or {}
        left_item = row.get("left_item") or {}
        right_item = row.get("right_item") or {}

        left_item_name = normalize_item_name(left_item.get("name"))
        right_item_name = normalize_item_name(right_item.get("name"))

        if script_mode == "no_items":
            left_item_name = ""
            right_item_name = ""

        elif script_mode == "partial":
            if not left_item_name or inventory_remaining.get(left_item_name, 0) <= 0:
                left_item_name = ""
            else:
                inventory_remaining[left_item_name] -= 1

            if not right_item_name or inventory_remaining.get(right_item_name, 0) <= 0:
                right_item_name = ""
            else:
                inventory_remaining[right_item_name] -= 1

        payload_rows.append(
            {
                "pair_key": str(row.get("pair_key") or "").strip(),
                "mode": str(row.get("mode") or "").strip(),
                "left_token_id": str(left.get("token_id") or "").strip(),
                "right_token_id": str(right.get("token_id") or "").strip(),
                "left_item_name": left_item_name,
                "right_item_name": right_item_name,
            }
        )

    return payload_rows


def build_apex_breeder_bookmarklet_code(
    planner_queue,
    script_mode="full",
    inventory_name_lookup=None,
    max_pairs=MAX_MASS_BREEDING_PAIRS,
):
    script_mode = str(script_mode or "full").strip().lower()
    if script_mode not in {"full", "partial", "no_items"}:
        script_mode = "full"

    payload_rows = build_bookmarklet_payload_rows(
        planner_queue,
        script_mode=script_mode,
        inventory_name_lookup=inventory_name_lookup,
        max_pairs=max_pairs,
    )
    payload_json = json.dumps(payload_rows, separators=(",", ":"))
    script_mode_json = json.dumps(script_mode)

    script = f"""
javascript:(async()=>{{
  const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
  const scriptMode = {script_mode_json};

  function textOf(el) {{
    return (el?.textContent || '').replace(/\\s+/g, ' ').trim();
  }}

  function isVisible(el) {{
    if (!el) return false;
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
  }}

  function visibleAll(selector, root = document) {{
    return Array.from(root.querySelectorAll(selector)).filter(isVisible);
  }}

  function findVisibleByText(selector, expectedText, root = document) {{
    const wanted = (expectedText || '').trim().toLowerCase();
    return visibleAll(selector, root).find(el => textOf(el).toLowerCase().includes(wanted)) || null;
  }}

  function getChickenDialog() {{
    const dialogs = visibleAll('section[role="dialog"]');
    for (const dlg of dialogs.reverse()) {{
      if (textOf(dlg).toLowerCase().includes('select a chicken to breed')) {{
        return dlg;
      }}
    }}
    return null;
  }}

  function getModeButton() {{
    return visibleAll('button[aria-haspopup="listbox"]').find(btn => {{
      const t = textOf(btn).toLowerCase();
      return t.includes('single breeding') || t.includes('mass breeding') || t.includes('remote breeding');
    }}) || null;
  }}

  async function ensureMassBreeding() {{
    const modeButton = getModeButton();
    if (!modeButton) throw new Error('Breeding mode selector not found.');

    const currentText = textOf(modeButton).toLowerCase();
    if (currentText.includes('mass breeding')) {{
      return;
    }}

    modeButton.click();
    await sleep(500);

    const massOption =
      document.querySelector('div[role="option"][data-key="mass"]') ||
      findVisibleByText('div[role="option"]', 'MASS BREEDING');

    if (!massOption) throw new Error('Mass Breeding option not found.');

    massOption.click();
    await sleep(900);
  }}

  function getEmptyParentSlots() {{
    return visibleAll('div.cursor-pointer').filter(el => {{
      const t = textOf(el).toLowerCase();
      return t.includes('select a chicken') && t.includes('# 0');
    }});
  }}

  async function openNextParentSlot() {{
    const slots = getEmptyParentSlots();
    if (!slots.length) throw new Error('No empty parent slot found.');

    const slot = slots[0];
    slot.scrollIntoView({{ block: 'center', behavior: 'instant' }});

    slot.click();
    await sleep(1000);

    let dialog = getChickenDialog();
    if (dialog) return;

    slot.dispatchEvent(new MouseEvent('click', {{
      bubbles: true,
      cancelable: true,
      view: window
    }}));
    await sleep(1000);

    dialog = getChickenDialog();
    if (dialog) return;

    throw new Error('Chicken selection dialog did not open.');
  }}

  async function fillChickenByToken(tokenId) {{
    await openNextParentSlot();

    const dialog = getChickenDialog();
    if (!dialog) throw new Error('Chicken selection dialog did not open.');

    const searchInput = dialog.querySelector('input[placeholder*="TOKEN ID"]');
    if (!searchInput) throw new Error('Chicken search input not found.');

    const nativeInputValueSetter = Object.getOwnPropertyDescriptor(
      window.HTMLInputElement.prototype,
      'value'
    ).set;

    searchInput.focus();

    nativeInputValueSetter.call(searchInput, '');
    searchInput.dispatchEvent(new Event('input', {{ bubbles: true }}));

    await sleep(200);

    nativeInputValueSetter.call(searchInput, String(tokenId).trim());
    searchInput.dispatchEvent(new Event('input', {{ bubbles: true }}));
    searchInput.dispatchEvent(new Event('change', {{ bubbles: true }}));

    const wantedTokenText = '#' + String(tokenId).trim();

    let targetCard = null;

    for (let attempt = 0; attempt < 20; attempt++) {{
      await sleep(300);

      const clickableCards = visibleAll('div.cursor-pointer.transition-transform', dialog);

      targetCard = clickableCards.find(card => {{
        const text = textOf(card);
        return text.includes(wantedTokenText);
      }});

      if (targetCard) {{
        break;
      }}
    }}

    if (!targetCard) {{
      throw new Error('Searched chicken card not found for ' + wantedTokenText);
    }}

    targetCard.click();
    await sleep(900);
  }}

  function getItemDialog() {{
    const dialogs = visibleAll('section[role="dialog"]');
    for (const dlg of dialogs.reverse()) {{
      const t = textOf(dlg).toLowerCase();
      if (t.includes('select items for parent')) {{
        return dlg;
      }}
    }}
    return null;
  }}

  function getItemSlotWrappers() {{
    return visibleAll('div.flex.flex-col.items-center.gap-2').filter(wrapper => {{
      const t = textOf(wrapper).toLowerCase();
      return t.includes('parent 1 items') || t.includes('parent 2 items');
    }});
  }}

  function getItemSlotButtonByPairIndex(pairIndex, slotLabel) {{
    const wrappers = getItemSlotWrappers();
    const normalized = String(slotLabel || '').trim().toLowerCase();

    const slotOffset = normalized === 'parent 2 items' ? 1 : 0;
    const wrapperIndex = (pairIndex * 2) + slotOffset;
    const wrapper = wrappers[wrapperIndex];

    if (!wrapper) {{
      return null;
    }}

    const addBtn = visibleAll('button', wrapper).find(btn => textOf(btn).toLowerCase() === 'add item');
    return addBtn || null;
  }}

  async function openItemSlot(pairIndex, slotLabel) {{
    const btn = getItemSlotButtonByPairIndex(pairIndex, slotLabel);
    if (!btn) throw new Error('Add Item button not found for pair #' + (pairIndex + 1) + ' ' + slotLabel);

    btn.scrollIntoView({{ block: 'center', behavior: 'instant' }});
    btn.click();
    await sleep(900);

    const dialog = getItemDialog();
    if (dialog) return;

    throw new Error('Item selection dialog did not open.');
  }}

  function getCloseItemDialogButton(dialog) {{
    if (!dialog) return null;

    return visibleAll('button', dialog).find(btn => {{
      const t = textOf(btn).toLowerCase();
      return t === 'close' || t === 'cancel' || t === 'x';
    }}) || null;
  }}

  async function closeItemDialogIfOpen() {{
    let dialog = getItemDialog();
    if (!dialog) return;

    const closeBtn = getCloseItemDialogButton(dialog);
    if (closeBtn) {{
      closeBtn.click();
      await sleep(500);
      return;
    }}

    document.dispatchEvent(new KeyboardEvent('keydown', {{
      key: 'Escape',
      code: 'Escape',
      keyCode: 27,
      which: 27,
      bubbles: true
    }}));
    await sleep(500);
  }}

  async function selectItemByName(itemName, pairIndex, slotLabel, strictItems) {{
    if (!itemName) return false;

    await openItemSlot(pairIndex, slotLabel);

    let dialog = getItemDialog();
    if (!dialog) {{
      if (strictItems) throw new Error('Item selection dialog not found.');
      return false;
    }}

    const wanted = (itemName || '').trim().toLowerCase();

    const itemButtons = visibleAll('button', dialog).filter(btn => {{
      const t = textOf(btn).toLowerCase();
      return t.includes(wanted);
    }});

    if (!itemButtons.length) {{
      if (strictItems) {{
        throw new Error('Item not found in modal: ' + itemName);
      }}

      await closeItemDialogIfOpen();
      return false;
    }}

    itemButtons[0].click();
    await sleep(700);

    dialog = getItemDialog();

    if (!dialog) {{
      return true;
    }}

    let doneButton =
      visibleAll('button', dialog).find(btn => textOf(btn).toLowerCase().includes('done')) ||
      visibleAll('button').find(btn => {{
        const t = textOf(btn).toLowerCase();
        return isVisible(btn) && t.includes('done');
      }});

    if (!doneButton) {{
      await sleep(500);
      dialog = getItemDialog();

      if (!dialog) {{
        return true;
      }}

      doneButton =
        visibleAll('button', dialog).find(btn => textOf(btn).toLowerCase().includes('done')) ||
        visibleAll('button').find(btn => {{
          const t = textOf(btn).toLowerCase();
          return isVisible(btn) && t.includes('done');
        }});
    }}

    if (!doneButton) {{
      if (strictItems) {{
        throw new Error('Done button not found in item dialog.');
      }}

      await closeItemDialogIfOpen();
      return false;
    }}

    doneButton.click();
    await sleep(900);
    return true;
  }}

  function getAddPairButton() {{
    return visibleAll('button').find(btn => textOf(btn).toLowerCase() === 'add pair') || null;
  }}

  async function clickAddPair() {{
    const btn = getAddPairButton();
    if (!btn) throw new Error('ADD PAIR button not found.');

    btn.scrollIntoView({{ block: 'center', behavior: 'instant' }});
    btn.click();
    await sleep(1200);
  }}

  async function fillPair(pair, pairIndex, strictItems) {{
    if (!pair || !pair.left_token_id || !pair.right_token_id) {{
      throw new Error('Planner pair is missing token IDs.');
    }}

    await fillChickenByToken(pair.left_token_id);
    await fillChickenByToken(pair.right_token_id);

    await selectItemByName(pair.left_item_name, pairIndex, 'parent 1 items', strictItems);
    await selectItemByName(pair.right_item_name, pairIndex, 'parent 2 items', strictItems);
  }}

  try {{
    alert('Automation has started. Click Ok to continue.');
    const currentUrl = window.location.href;
    if (!currentUrl.includes('app.chickensaga.com/breeding')) {{
      alert('Open the Chicken Saga breeding page first, then click this bookmark.');
      return;
    }}

    const plannerPairs = {payload_json};

    if (!plannerPairs.length) {{
      alert('Your breeding planner is empty.');
      return;
    }}

    window.__APEX_BREEDER_QUEUE__ = plannerPairs;

    const firstPair = plannerPairs[0];
    if (!firstPair.left_token_id || !firstPair.right_token_id) {{
      throw new Error('First planner pair is missing token IDs.');
    }}

    const pairCountToFill = plannerPairs.length;
    const strictItems = scriptMode === 'full';

    await ensureMassBreeding();
    await sleep(500);

    for (let i = 0; i < pairCountToFill; i++) {{
      const pair = plannerPairs[i];
      await fillPair(pair, i, strictItems);

      if (i < pairCountToFill - 1) {{
        await clickAddPair();
      }}
    }}

    let doneMessage = 'Apex Breeder: Planner autofill completed.\\n\\n' +
      'Mode: ' + scriptMode + '\\n' +
      'Filled pairs: ' + pairCountToFill + '\\n\\n' +
      'Review everything manually before final confirmation.';

    if (scriptMode === 'partial') {{
      doneMessage = 'Apex Breeder: Partial planner autofill completed.\\n\\n' +
        'Filled pairs: ' + pairCountToFill + '\\n' +
        'Missing items were skipped.\\n\\n' +
        'Review everything manually before final confirmation.';
    }}

    if (scriptMode === 'no_items') {{
      doneMessage = 'Apex Breeder: No-items planner autofill completed.\\n\\n' +
        'Filled pairs: ' + pairCountToFill + '\\n' +
        'All items were skipped.\\n\\n' +
        'Review everything manually before final confirmation.';
    }}

    alert(doneMessage);
  }} catch (error) {{
    alert('Bookmarklet failed: ' + (error && error.message ? error.message : error));
  }}
}})();
""".strip()

    return script

def build_bookmarklet_inventory_name_lookup(wallet_address):
    wallet_address = str(wallet_address or "").strip().lower()
    if not wallet_address:
        return {}

    return build_wallet_inventory_name_lookup(wallet_address)
