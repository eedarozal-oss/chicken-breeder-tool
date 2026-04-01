document.addEventListener("DOMContentLoaded", function () {
    const detailButtons = document.querySelectorAll(".toggle-details-btn");
    const clickableRows = document.querySelectorAll(".clickable-row");

    const compareModal = document.getElementById("compare-modal");
    const compareModalBody = document.getElementById("compare-modal-body");
    const compareOpenButtons = document.querySelectorAll(".open-compare-btn");
    const compareCloseButtons = document.querySelectorAll("[data-close-compare]");
    const autoOpenTemplateId = (document.body?.dataset?.autoOpenTemplateId || "").trim();

    const autoMatchConfigModal = document.getElementById("auto-match-config-modal");
    const autoMatchConfigCloseButtons = document.querySelectorAll("[data-close-auto-match-config]");
    const autoMatchResultModal = document.getElementById("auto-match-result-modal");
    const autoMatchResultCloseButtons = document.querySelectorAll("[data-close-auto-match-result]");
    const autoMatchOpenButtons = document.querySelectorAll(".open-auto-match-config-btn");

    function hideAllDetails() {
        document.querySelectorAll(".details-row").forEach((row) => {
            row.classList.add("hidden");
        });

        document.querySelectorAll(".toggle-details-btn").forEach((btn) => {
            const btnOpenLabel = btn.getAttribute("data-open-label") || "View Details";
            btn.textContent = btnOpenLabel;
        });
    }

    detailButtons.forEach((button) => {
        const openLabel = button.getAttribute("data-open-label") || button.textContent.trim();

        button.addEventListener("click", function () {
            const targetId = button.getAttribute("data-target");
            const detailsRow = document.getElementById(targetId);

            if (!detailsRow) return;

            const isHidden = detailsRow.classList.contains("hidden");

            hideAllDetails();

            if (isHidden) {
                detailsRow.classList.remove("hidden");
                button.textContent = "Hide";
            } else {
                button.textContent = openLabel;
            }
        });
    });

    clickableRows.forEach((row) => {
        row.addEventListener("click", function (event) {
            if (event.target.closest("a, button, input, select, textarea, label, form")) return;

            if (row.dataset.href) {
                window.location.href = row.dataset.href;
                return;
            }

            if (row.dataset.target) {
                const detailsRow = document.getElementById(row.dataset.target);
                if (!detailsRow) return;

                const isHidden = detailsRow.classList.contains("hidden");

                hideAllDetails();

                if (isHidden) {
                    detailsRow.classList.remove("hidden");
                }
            }
        });

        row.addEventListener("keydown", function (event) {
            if (event.key !== "Enter" && event.key !== " ") return;

            event.preventDefault();

            if (row.dataset.href) {
                window.location.href = row.dataset.href;
                return;
            }

            if (row.dataset.target) {
                const detailsRow = document.getElementById(row.dataset.target);
                if (!detailsRow) return;

                const isHidden = detailsRow.classList.contains("hidden");

                hideAllDetails();

                if (isHidden) {
                    detailsRow.classList.remove("hidden");
                }
            }
        });
    });

    function openCompareModal(templateId) {
        if (!compareModal || !compareModalBody) return;

        const template = document.getElementById(templateId);
        if (!template) return;

        compareModalBody.innerHTML = template.innerHTML;
        compareModal.classList.remove("hidden");
        document.body.classList.add("modal-open");
    }

    function closeCompareModal() {
        if (!compareModal || !compareModalBody) return;

        compareModal.classList.add("hidden");
        compareModalBody.innerHTML = "";
        document.body.classList.remove("modal-open");
    }

    compareOpenButtons.forEach((button) => {
        button.addEventListener("click", function () {
            const templateId = button.getAttribute("data-template-id");
            openCompareModal(templateId);
        });
    });

    compareCloseButtons.forEach((button) => {
        button.addEventListener("click", function () {
            closeCompareModal();
        });
    });

    function openAutoMatchConfigModal() {
        if (!autoMatchConfigModal) return;
        autoMatchConfigModal.classList.remove("hidden");
        document.body.classList.add("modal-open");
        syncAutoMatchConfigState();
    }

    function closeAutoMatchConfigModal() {
        if (!autoMatchConfigModal) return;
        autoMatchConfigModal.classList.add("hidden");
        document.body.classList.remove("modal-open");
    }

    function openAutoMatchResultModal() {
        if (!autoMatchResultModal) return;
        autoMatchResultModal.classList.remove("hidden");
        document.body.classList.add("modal-open");
    }

	function closeAutoMatchResultModal(reopenConfig = true) {
		if (!autoMatchResultModal) return;

		autoMatchResultModal.classList.add("hidden");

		if (reopenConfig && autoMatchConfigModal) {
			setTimeout(function () {
				autoMatchConfigModal.classList.remove("hidden");
				document.body.classList.add("modal-open");
			}, 0);
		} else {
			document.body.classList.remove("modal-open");
		}
	}

    autoMatchOpenButtons.forEach((button) => {
        button.addEventListener("click", function (event) {
            event.preventDefault();
            openAutoMatchConfigModal();
        });
    });	

    autoMatchConfigCloseButtons.forEach((button) => {
        button.addEventListener("click", function () {
            closeAutoMatchConfigModal();
        });
    });

	autoMatchResultCloseButtons.forEach((button) => {
		button.addEventListener("click", function (event) {
			event.preventDefault();
			event.stopPropagation();
			closeAutoMatchResultModal(true);
		});
	});

    function syncAutoMatchConfigState() {
        const modeInputs = document.querySelectorAll('input[name="auto_match_mode"]');
        const countWrap = document.querySelector("[data-auto-match-count-wrap]");
        const countInput = document.querySelector('input[name="multi_count"]');

        if (!modeInputs.length) return;

        let selectedMode = "single";
        modeInputs.forEach((input) => {
            if (input.checked) {
                selectedMode = input.value;
            }
        });

        const isMultiple = selectedMode === "multiple";

        if (countWrap) {
            countWrap.classList.toggle("hidden", !isMultiple);
        }

        if (countInput) {
            countInput.disabled = !isMultiple;
        }
    }

    document.querySelectorAll('input[name="auto_match_mode"]').forEach((input) => {
        input.addEventListener("change", syncAutoMatchConfigState);
    });

    syncAutoMatchConfigState();

	document.addEventListener("keydown", function (event) {
		if (event.key !== "Escape") return;

		if (autoMatchResultModal && !autoMatchResultModal.classList.contains("hidden")) {
			closeAutoMatchResultModal(true);
			return;
		}

		if (autoMatchConfigModal && !autoMatchConfigModal.classList.contains("hidden")) {
			closeAutoMatchConfigModal();
			return;
		}

		closeCompareModal();
	});

    if (autoOpenTemplateId) {
        openCompareModal(autoOpenTemplateId);
    }

    if (document.body?.dataset?.autoMatchResultOpen === "1") {
        openAutoMatchResultModal();
    }
});