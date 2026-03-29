(function () {
  const selected = new Set();

  function refreshSelectionUi(root) {
    const countNode = root.querySelector("[data-selection-count]");
    const hidden = root.querySelector("[data-selection-target]");
    const submit = root.querySelector("[data-selection-submit]");
    if (countNode) {
      countNode.textContent = selected.size + " selected";
    }
    if (hidden) {
      hidden.value = Array.from(selected).sort((a, b) => Number(a) - Number(b)).join(",");
    }
    if (submit) {
      submit.disabled = selected.size === 0;
    }
  }

  function syncVisibleCheckboxes(root) {
    root.querySelectorAll("[data-store-checkbox]").forEach((checkbox) => {
      checkbox.checked = selected.has(String(checkbox.value));
    });
    const pageToggle = root.querySelector("[data-select-page]");
    if (pageToggle) {
      const pageBoxes = Array.from(root.querySelectorAll("[data-store-checkbox]"));
      pageToggle.checked = pageBoxes.length > 0 && pageBoxes.every((box) => box.checked);
    }
  }

  function bind(root) {
    const selectionRoot = document.querySelector("[data-selection-root]");
    if (!selectionRoot) {
      return;
    }
    root.querySelectorAll("[data-store-checkbox]").forEach((checkbox) => {
      checkbox.addEventListener("change", () => {
        const value = String(checkbox.value);
        if (checkbox.checked) {
          selected.add(value);
        } else {
          selected.delete(value);
        }
        refreshSelectionUi(selectionRoot);
        syncVisibleCheckboxes(selectionRoot);
      });
    });

    const pageToggle = root.querySelector("[data-select-page]");
    if (pageToggle) {
      pageToggle.addEventListener("change", () => {
        root.querySelectorAll("[data-store-checkbox]").forEach((checkbox) => {
          checkbox.checked = pageToggle.checked;
          const value = String(checkbox.value);
          if (pageToggle.checked) {
            selected.add(value);
          } else {
            selected.delete(value);
          }
        });
        refreshSelectionUi(selectionRoot);
      });
    }

    syncVisibleCheckboxes(selectionRoot);
    refreshSelectionUi(selectionRoot);
  }

  document.addEventListener("DOMContentLoaded", () => {
    bind(document);
  });

  document.body.addEventListener("htmx:afterSwap", (event) => {
    bind(event.target);
  });
})();
