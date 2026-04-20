document.addEventListener("DOMContentLoaded", function () {
    let activeNoticeTimer = null;
    const detailButtons = document.querySelectorAll(".toggle-details-btn");
    const clickableRows = document.querySelectorAll(".clickable-row");

    const compareModal = document.getElementById("compare-modal");
    const compareModalBody = document.getElementById("compare-modal-body");
    const compareOpenButtons = document.querySelectorAll(".open-compare-btn");
    const compareCloseButtons = document.querySelectorAll("[data-close-compare]");
    const autoOpenTemplateId = (document.body?.dataset?.autoOpenTemplateId || "").trim();

    const compareAddForm = document.getElementById("compare-add-form");

    const autoMatchConfigModal = document.getElementById("auto-match-config-modal");
    const autoMatchConfigCloseButtons = document.querySelectorAll("[data-close-auto-match-config]");
    const autoMatchResultModal = document.getElementById("auto-match-result-modal");
    const autoMatchResultCloseButtons = document.querySelectorAll("[data-close-auto-match-result]");
    const autoMatchOpenButtons = document.querySelectorAll(".open-auto-match-config-btn");
    const autoMatchConfigForms = document.querySelectorAll(".auto-match-config-form");

    const plannerModal = document.getElementById("planner-modal");
    const plannerOpenButtons = document.querySelectorAll(".open-planner-btn");
    const plannerCloseButtons = document.querySelectorAll("[data-close-planner]");

	let plannerStateChanged = false;
	let plannerRemovalChanged = false;

    function showPageNotice(message, tone = "info") {
        if (!message) return;

        const existing = document.getElementById("page-feedback-notice");
        if (existing) {
            existing.remove();
        }

        const notice = document.createElement("div");
        notice.id = "page-feedback-notice";
        notice.className = `floating-notice floating-notice-${tone}`;
        notice.setAttribute("role", tone === "error" ? "alert" : "status");
        notice.textContent = message;
        document.body.appendChild(notice);

        if (activeNoticeTimer) {
            clearTimeout(activeNoticeTimer);
        }

        activeNoticeTimer = setTimeout(function () {
            notice.classList.add("is-hiding");
            setTimeout(function () {
                notice.remove();
            }, 180);
        }, 2400);
    }

    function readCookie(name) {
        const cookiePrefix = `${name}=`;
        const cookies = document.cookie ? document.cookie.split(";") : [];

        for (const rawCookie of cookies) {
            const cookie = rawCookie.trim();
            if (cookie.startsWith(cookiePrefix)) {
                return decodeURIComponent(cookie.slice(cookiePrefix.length));
            }
        }

        return "";
    }

    function getCsrfToken() {
        return readCookie("apex_csrf_token");
    }

    function appendCsrfToken(form) {
        if (!form) return;

        const csrfToken = getCsrfToken();
        if (!csrfToken) return;

        let input = form.querySelector('input[name="csrf_token"]');
        if (!input) {
            input = document.createElement("input");
            input.type = "hidden";
            input.name = "csrf_token";
            form.appendChild(input);
        }

        input.value = csrfToken;
    }

    document.querySelectorAll('form[method="POST"]').forEach((form) => {
        appendCsrfToken(form);
    });

	function reloadPageWithoutModalState() {
		const url = new URL(window.location.href);

		url.searchParams.delete("auto_match");
		url.searchParams.delete("auto_match_mode");
		url.searchParams.delete("auto_match_source");
		url.searchParams.delete("auto_open_template_id");
		url.searchParams.delete("popup_match_count");
		url.searchParams.delete("popup_ip_diff");
		url.searchParams.delete("popup_breed_diff");
		url.searchParams.delete("popup_ninuno");
		url.searchParams.delete("popup_build");
		url.searchParams.delete("popup_min_build_count");
		url.searchParams.delete("popup_same_build");
		url.searchParams.delete("popup_same_instinct");

		url.searchParams.set("skip_auto_open", "1");

		window.location.href = url.toString();
	}
	
	function reloadPageForPlannerOpen() {
		sessionStorage.setItem("reopenPlannerModal", "1");
		reloadPageWithoutModalState();
	}

    function lockBody() {
        document.body.classList.add("modal-open");
    }

    function unlockBodyIfNoModalOpen() {
        const anyOpenModal = document.querySelector(".compare-modal:not(.hidden)");
        if (!anyOpenModal) {
            document.body.classList.remove("modal-open");
        }
    }

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

    function fillCompareAddForm(template) {
        if (!compareAddForm || !template) return;

        const leftTokenId = (template.dataset.leftTokenId || "").trim();
        const rightTokenId = (template.dataset.rightTokenId || "").trim();

        compareAddForm.querySelector('input[name="left_token_id"]').value = leftTokenId;
        compareAddForm.querySelector('input[name="right_token_id"]').value = rightTokenId;

        const leftItemName = (template.dataset.leftItemName || "").trim();
        const rightItemName = (template.dataset.rightItemName || "").trim();
        const pairQuality = (template.dataset.pairQuality || "").trim();

        compareAddForm.querySelector('input[name="left_item_name"]').value = leftItemName;
        compareAddForm.querySelector('input[name="right_item_name"]').value = rightItemName;
        compareAddForm.querySelector('input[name="pair_quality"]').value = pairQuality;
    }
	
	function clearPlannerEmptyState() {
		const emptyState = document.querySelector("#planner-modal .empty-state-card");
		if (emptyState) {
			emptyState.remove();
		}
	}
	
    function clearCompareAddForm() {
        if (!compareAddForm) return;

        const fields = [
            'input[name="left_token_id"]',
            'input[name="right_token_id"]',
            'input[name="left_item_name"]',
            'input[name="right_item_name"]',
            'input[name="pair_quality"]'
        ];

        fields.forEach((selector) => {
            const input = compareAddForm.querySelector(selector);
            if (input) input.value = "";
        });
    }

    function openCompareModal(templateId) {
        if (!compareModal || !compareModalBody) return;

        const template = document.getElementById(templateId);
        if (!template) return;

        compareModalBody.innerHTML = template.innerHTML;
        fillCompareAddForm(template);
        compareModal.classList.remove("hidden");
        lockBody();
    }

	function closeCompareModal() {
		if (!compareModal || !compareModalBody) return;

		compareModal.classList.add("hidden");
		compareModalBody.innerHTML = "";
		clearCompareAddForm();
		unlockBodyIfNoModalOpen();
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
        lockBody();
        syncAutoMatchConfigState();
    }

    function closeAutoMatchConfigModal() {
        if (!autoMatchConfigModal) return;
        autoMatchConfigModal.classList.add("hidden");
        autoMatchConfigForms.forEach(function (form) {
            form.dataset.submitting = "0";
            const submitButton = form.querySelector('button[type="submit"]');
            if (!submitButton) return;

            submitButton.disabled = false;
            submitButton.classList.remove("is-pending");
            submitButton.textContent = submitButton.dataset.defaultLabel || submitButton.textContent;
        });
        unlockBodyIfNoModalOpen();
    }

    function openAutoMatchResultModal() {
        if (!autoMatchResultModal) return;
        autoMatchResultModal.classList.remove("hidden");
        lockBody();
    }

	function closeAutoMatchResultModal(reopenConfig = true) {
		if (!autoMatchResultModal) return;

		autoMatchResultModal.classList.add("hidden");

		if (reopenConfig && autoMatchConfigModal) {
			setTimeout(function () {
				autoMatchConfigModal.classList.remove("hidden");
				lockBody();
			}, 0);
		} else {
			unlockBodyIfNoModalOpen();
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
			reloadPageWithoutModalState();
		});
	});

	function openPlannerModal() {
		if (!plannerModal) return;

		if (plannerStateChanged) {
			reloadPageForPlannerOpen();
			return;
		}

		plannerModal.classList.remove("hidden");
		lockBody();
	}

	function closePlannerModal() {
		if (!plannerModal) return;

		plannerModal.classList.add("hidden");
		unlockBodyIfNoModalOpen();

		if (plannerRemovalChanged) {
			reloadPageWithoutModalState();
		}
	}

    plannerOpenButtons.forEach((button) => {
        button.addEventListener("click", function (event) {
            event.preventDefault();
            openPlannerModal();
        });
    });

    plannerCloseButtons.forEach((button) => {
        button.addEventListener("click", function (event) {
            event.preventDefault();
            closePlannerModal();
        });
    });

    function syncAutoMatchConfigState() {
        const modeSelect = document.querySelector('select[name="auto_match_mode"]');
        const modeInputs = document.querySelectorAll('input[name="auto_match_mode"]');
        const hasModeControl = Boolean(modeSelect) || Boolean(modeInputs.length);
        const countWrap =
            document.querySelector("[data-auto-match-count-wrap]") ||
            document.querySelector(".multi-match-count-wrap");
        const sameBuildInput = document.querySelector('input[name="popup_same_build"]');
        const sameInstinctInput = document.querySelector('input[name="popup_same_instinct"]');

        const countInput =
            document.querySelector('input[name="multi_count"]') ||
            document.querySelector('input[name="popup_match_count"]');

        let selectedMode = "single";

        if (modeSelect) {
            selectedMode = modeSelect.value;
        } else if (modeInputs.length) {
            modeInputs.forEach((input) => {
                if (input.checked) selectedMode = input.value;
            });
        }

        const isMultiple = !hasModeControl || selectedMode === "multiple";

        if (countWrap) countWrap.classList.toggle("hidden", !isMultiple);
        if (countInput) countInput.disabled = !isMultiple;
        if (sameBuildInput) sameBuildInput.disabled = !isMultiple;
        if (sameInstinctInput) sameInstinctInput.disabled = !isMultiple;
    }

    document.querySelectorAll('input[name="auto_match_mode"], select[name="auto_match_mode"]').forEach((input) => {
        input.addEventListener("change", syncAutoMatchConfigState);
    });

    syncAutoMatchConfigState();

    autoMatchConfigForms.forEach(function (form) {
        const submitButton = form.querySelector('button[type="submit"]');
        if (!submitButton) return;

        submitButton.dataset.defaultLabel = submitButton.textContent.trim() || "Run Auto Match";

        form.addEventListener("submit", function () {
            if (form.dataset.submitting === "1") {
                return;
            }

            form.dataset.submitting = "1";
            submitButton.disabled = true;
            submitButton.classList.add("is-pending");
            submitButton.textContent = "Running...";
        });
    });

    function markPlannerButtonAdded(button) {
        if (!button) return;
        button.disabled = true;
        button.dataset.plannerState = "added";
        button.classList.add("is-disabled", "is-added");
        button.setAttribute("aria-disabled", "true");
        button.setAttribute("title", "Already added to breeding planner");
    }

    function markPlannerButtonRemoved(button) {
        if (!button) return;
        button.disabled = true;
        button.dataset.plannerState = "removed";
        button.classList.add("is-disabled", "is-removed");
        button.setAttribute("aria-disabled", "true");
        button.setAttribute("title", "Already removed from breeding planner");
    }

	function updateAvailablePairMax(delta) {
		const note = document.querySelector(".multi-match-count-wrap .note");
		const countInput = document.querySelector('input[name="popup_match_count"]');

		if (!note) return;

		const match = note.textContent.match(/(\d+)/);
		if (!match) return;

		const current = parseInt(match[1], 10);
		if (Number.isNaN(current)) return;

		const next = Math.max(0, current + delta);

		note.textContent = `Max from current pool: ${next}`;

		if (countInput) {
			countInput.max = String(next);

			const currentValue = parseInt(countInput.value || "0", 10);
			if (!Number.isNaN(currentValue) && currentValue > next) {
				countInput.value = String(Math.max(1, next));
			}
		}
	}

	function updatePlannerCount(delta) {
		const heroCount = document.querySelector(".breeding-planner-count");
		const modalCount = document.querySelector(".planner-modal-count strong");

		if (heroCount) {
			const current = parseInt(heroCount.textContent || "0", 10);
			if (!Number.isNaN(current)) {
				heroCount.textContent = String(Math.max(0, current + delta));
			}
		}

		if (modalCount) {
			const current = parseInt(modalCount.textContent || "0", 10);
			if (!Number.isNaN(current)) {
				modalCount.textContent = String(Math.max(0, current + delta));
			}
		}
	}
	
	function removePlannerRow(button) {
		const row = button.closest(".planner-row");
		if (!row) return;

		row.remove();

		const remainingRows = document.querySelectorAll("#planner-modal .planner-row");
		const emptyState = document.querySelector("#planner-modal .empty-state-card");

		if (remainingRows.length === 0 && !emptyState) {
			const compareBody = document.querySelector("#planner-modal .compare-body");
			if (compareBody) {
				const emptyCard = document.createElement("div");
				emptyCard.className = "empty-state-card empty-state-card-inline";
				emptyCard.innerHTML = `
					<div class="empty-state-kicker">Planner is empty</div>
					<h3>No queued pairs yet.</h3>
					<p>You can manually review a match or use Auto Match, then add the pair to the breeding planner.</p>
				`;
				compareBody.appendChild(emptyCard);
			}
		}
	}

	function reloadWithoutSelectedChicken() {
		const url = new URL(window.location.href);

		url.searchParams.delete("selected_token_id");
		url.searchParams.delete("auto_match");
		url.searchParams.delete("auto_match_mode");
		url.searchParams.delete("auto_match_source");
		url.searchParams.delete("auto_open_template_id");
		url.searchParams.delete("popup_match_count");
		url.searchParams.delete("popup_ip_diff");
		url.searchParams.delete("popup_breed_diff");
		url.searchParams.delete("popup_ninuno");
		url.searchParams.delete("popup_build");
		url.searchParams.delete("popup_min_build_count");
		url.searchParams.delete("popup_same_build");
		url.searchParams.delete("popup_same_instinct");

		url.hash = "selected-chicken";
		window.location.href = url.toString();
	}

    async function submitPlannerFormAjax(form, button, mode) {
        if (!form || !button || button.disabled || form.dataset.submitting === "1") return;

        appendCsrfToken(form);

        form.dataset.submitting = "1";
        button.disabled = true;
        button.classList.add("is-pending");

        try {
            const response = await fetch(form.action, {
                method: (form.method || "POST").toUpperCase(),
                body: new FormData(form),
                headers: {
                    "X-Requested-With": "XMLHttpRequest",
                    "X-CSRF-Token": getCsrfToken()
                },
                credentials: "same-origin",
                redirect: "follow"
            });

            if (!response.ok) {
                throw new Error("Planner update failed");
            }

			if (mode === "add") {
				markPlannerButtonAdded(button);
				updatePlannerCount(1);
				updateAvailablePairMax(-1);
				clearPlannerEmptyState();
                showPageNotice("Pair added to breeding planner.", "success");

				const isSelectedMatchFlow =
					form.id === "compare-add-form" ||
					!!form.closest("#compare-modal");

				form.dataset.submitting = "0";
				button.classList.remove("is-pending");
				plannerStateChanged = true;

				if (isSelectedMatchFlow) {
					reloadWithoutSelectedChicken();
					return;
				}
			} else {
				markPlannerButtonRemoved(button);
				updatePlannerCount(-1);
				updateAvailablePairMax(1);
				removePlannerRow(button);
                showPageNotice("Pair removed from breeding planner.", "success");

				plannerStateChanged = true;
				plannerRemovalChanged = true;
				form.dataset.submitting = "0";
				button.classList.remove("is-pending");
			}
			
        } catch (error) {
            button.disabled = false;
            button.classList.remove("is-pending");
            form.dataset.submitting = "0";
            console.error(error);
            showPageNotice("Planner update failed. Please try again.", "error");
        }
    }

    function bindPlannerAjaxForms(scope = document) {
        const forms = scope.querySelectorAll("form");

        forms.forEach((form) => {
            if (form.dataset.ajaxPlannerBound === "1") return;

            const addButton = form.querySelector(".planner-add-btn");
            const removeButton = form.querySelector(".planner-remove-icon-btn");

            if (!addButton && !removeButton) return;

            form.dataset.ajaxPlannerBound = "1";

            form.addEventListener("submit", function (event) {
                if (addButton) {
                    event.preventDefault();
                    event.stopPropagation();
                    submitPlannerFormAjax(form, addButton, "add");
                    return;
                }

                if (removeButton) {
                    event.preventDefault();
                    event.stopPropagation();
                    submitPlannerFormAjax(form, removeButton, "remove");
                }
            });
        });
    }

    bindPlannerAjaxForms(document);

    document.addEventListener("keydown", function (event) {
        if (event.key !== "Escape") return;

        if (plannerModal && !plannerModal.classList.contains("hidden")) {
            closePlannerModal();
            return;
        }

		if (autoMatchResultModal && !autoMatchResultModal.classList.contains("hidden")) {
			reloadPageWithoutModalState();
			return;
		}

        if (autoMatchConfigModal && !autoMatchConfigModal.classList.contains("hidden")) {
            closeAutoMatchConfigModal();
            return;
        }

        if (compareModal && !compareModal.classList.contains("hidden")) {
            closeCompareModal();
        }
    });

	const reopenPlannerModal = sessionStorage.getItem("reopenPlannerModal") === "1";
	if (reopenPlannerModal) {
		sessionStorage.removeItem("reopenPlannerModal");
		openPlannerModal();
	}

	const urlParams = new URLSearchParams(window.location.search);
	const shouldOpenPlannerFromUrl = urlParams.get("open_planner") === "1";

	if (shouldOpenPlannerFromUrl) {
		openPlannerModal();

		urlParams.delete("open_planner");
		const nextQuery = urlParams.toString();
		const nextUrl = nextQuery
			? `${window.location.pathname}?${nextQuery}${window.location.hash || ""}`
			: `${window.location.pathname}${window.location.hash || ""}`;

		window.history.replaceState({}, "", nextUrl);
	}

    if (autoOpenTemplateId) {
        openCompareModal(autoOpenTemplateId);
    }

    if (document.body?.dataset?.autoMatchResultOpen === "1") {
        openAutoMatchResultModal();
    }

    const autoMatchEmptyModal = document.getElementById("auto-match-empty-modal");
    if (autoMatchEmptyModal) {
        lockBody();

        function closeAutoMatchEmptyModal() {
            autoMatchEmptyModal.remove();
            unlockBodyIfNoModalOpen();
        }

        autoMatchEmptyModal.querySelectorAll("[data-close-auto-match-empty]").forEach(function (node) {
            node.addEventListener("click", closeAutoMatchEmptyModal);
        });
    }

    document.querySelectorAll(".ip-column-filter-form").forEach(function (form) {
        form.addEventListener("submit", function () {
            form.querySelectorAll(".ip-filter-csv-source").forEach(function (checkbox) {
                const targetId = checkbox.getAttribute("data-target-input");
                if (!targetId) return;

                const hiddenInput = form.querySelector("#" + targetId);
                if (!hiddenInput) return;

                const selectedValues = Array.from(
                    form.querySelectorAll('.ip-filter-csv-source[data-target-input="' + targetId + '"]:checked')
                ).map(function (node) {
                    return node.value;
                });

                hiddenInput.value = selectedValues.join(",");
            });

            const currentAction = form.getAttribute("action") || window.location.pathname;
            form.setAttribute("action", currentAction.split("#")[0] + "#available-chickens");
        });
    });

    function initBuildPairMax(options) {
        const popupBuild = document.getElementById(options.popupBuildId);
        const popupMinBuildCount = document.getElementById(options.popupMinBuildCountId);
        const popupMatchCount = document.getElementById(options.popupMatchCountId);
        const pairMaxText = document.getElementById(options.pairMaxId);
        const tableRows = Array.from(document.querySelectorAll(options.tableRowSelector));

        if (!popupBuild || !popupMinBuildCount || !popupMatchCount || !pairMaxText || !tableRows.length) {
            return;
        }

        const compatibility = {
            "killua": new Set(["killua", "hybrid 1", "hybrid 2"]),
            "shanks": new Set(["shanks", "hybrid 1"]),
            "levi": new Set(["levi", "hybrid 1", "hybrid 2"]),
            "hybrid 1": new Set(["killua", "shanks", "levi", "hybrid 1"]),
            "hybrid 2": new Set(["killua", "levi", "hybrid 2"]),
        };

        function buildsAreCompatible(selectedBuild, rowBuild) {
            const left = String(selectedBuild || "").trim().toLowerCase();
            const right = String(rowBuild || "").trim().toLowerCase();

            if (!left || !right) return false;

            const leftCompatible = compatibility[left] || new Set();
            const rightCompatible = compatibility[right] || new Set();

            return leftCompatible.has(right) && rightCompatible.has(left);
        }

        function getVisibleRows() {
            return tableRows.filter(function (row) {
                return row.offsetParent !== null;
            });
        }

        function recomputeAvailablePairMax() {
            const selectedBuild = String(popupBuild.value || "all").trim().toLowerCase();
            const minBuildCountRaw = String(popupMinBuildCount.value || "").trim();
            const minBuildCount = minBuildCountRaw === "" ? null : parseInt(minBuildCountRaw, 10);

            let eligibleCount = 0;

            getVisibleRows().forEach(function (row) {
                const rowBuild = String(row.dataset.buildKey || "").trim().toLowerCase();
                const rowBuildCount = parseInt(row.dataset.buildCount || "0", 10) || 0;

                if (selectedBuild !== "all" && !buildsAreCompatible(selectedBuild, rowBuild)) {
                    return;
                }

                if (minBuildCount !== null && rowBuildCount < minBuildCount) {
                    return;
                }

                eligibleCount += 1;
            });

            const pairMax = Math.max(0, Math.floor(eligibleCount / 2));

            pairMaxText.textContent = String(pairMax);
            popupMatchCount.max = String(pairMax);

            const currentValue = parseInt(popupMatchCount.value || "1", 10) || 1;

            if (pairMax === 0) {
                popupMatchCount.value = "1";
            } else if (currentValue > pairMax) {
                popupMatchCount.value = String(pairMax);
            } else if (currentValue < 1) {
                popupMatchCount.value = "1";
            }
        }

        popupBuild.addEventListener("change", recomputeAvailablePairMax);
        popupMinBuildCount.addEventListener("input", recomputeAvailablePairMax);
        popupMinBuildCount.addEventListener("change", recomputeAvailablePairMax);

        recomputeAvailablePairMax();
    }

    initBuildPairMax({
        popupBuildId: "gene-popup-build",
        popupMinBuildCountId: "gene-popup-min-build-count",
        popupMatchCountId: "gene-popup-match-count",
        pairMaxId: "gene-available-pair-max",
        tableRowSelector: "#gene-available-table tbody tr.clickable-row",
    });

    initBuildPairMax({
        popupBuildId: "ultimate-popup-build",
        popupMinBuildCountId: "ultimate-popup-min-build-count",
        popupMatchCountId: "ultimate-popup-match-count",
        pairMaxId: "ultimate-available-pair-max",
        tableRowSelector: "#ultimate-available-table tbody tr.clickable-row",
    });

    (function initGeneBatchLoader() {
        const batchWrap = document.getElementById("gene-batch-wrap");
        const statusBox = document.getElementById("gene-batch-status");
        const toggleBtn = document.getElementById("toggle-gene-batch");

        if (!statusBox || !toggleBtn) {
            return;
        }

        const wallet = statusBox.dataset.wallet || "";
        const selectedTokenId = statusBox.dataset.selectedTokenId || "";
        const batchUrl = statusBox.dataset.batchUrl || "";
        const pauseKey = `gene-batch-paused-${wallet}`;
        let running = localStorage.getItem(pauseKey) !== "1";
        let timerId = null;

        function updateBatchWrapVisibility() {
            if (!batchWrap) return;

            const statusHidden = statusBox.style.display === "none";
            const buttonHidden = toggleBtn.style.display === "none";
            batchWrap.style.display = (statusHidden && buttonHidden) ? "none" : "";
        }

        function setRunning(next) {
            running = next;
            toggleBtn.textContent = running ? "Stop Recessive Check" : "Resume Recessive Check";
            statusBox.dataset.running = running ? "1" : "0";

            if (running) {
                statusBox.style.display = "";
                toggleBtn.style.display = "";
                statusBox.textContent = "Recessive gene data is loading in the background...";
                updateBatchWrapVisibility();
                scheduleNext(0);
            } else {
                statusBox.textContent = "Recessive gene background loading is paused.";
                statusBox.style.display = "";
                toggleBtn.style.display = "";
                updateBatchWrapVisibility();

                if (timerId) {
                    clearTimeout(timerId);
                    timerId = null;
                }
            }
        }

        function scheduleNext(delayMs) {
            if (!running) return;
            if (timerId) clearTimeout(timerId);
            timerId = setTimeout(runBatch, delayMs);
        }

        async function runBatch() {
            if (!running || !batchUrl) return;

            const formData = new URLSearchParams();
            formData.append("wallet_address", wallet);
            formData.append("selected_token_id", selectedTokenId);

            try {
                const response = await fetch(batchUrl, {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/x-www-form-urlencoded",
                        "X-CSRF-Token": getCsrfToken(),
                    },
                    body: formData.toString(),
                });

                const data = await response.json();

                if (!data.ok) {
                    statusBox.textContent = "Background loading stopped due to an error.";
                    running = false;
                    toggleBtn.textContent = "Resume Recessive Check";
                    showPageNotice("Gene background loading stopped due to an error.", "error");
                    updateBatchWrapVisibility();
                    return;
                }

                if ((data.remaining || 0) > 0) {
                    statusBox.style.display = "";
                    toggleBtn.style.display = "";
                    statusBox.textContent = `Recessive gene data is loading in the background. Loaded ${data.loaded || 0} chickens this round. ${data.remaining || 0} still remaining.`;
                    updateBatchWrapVisibility();
                    scheduleNext(2500);
                } else {
                    running = false;
                    localStorage.removeItem(pauseKey);
                    statusBox.textContent = "";
                    statusBox.style.display = "none";
                    toggleBtn.style.display = "none";
                    updateBatchWrapVisibility();
                }
            } catch (err) {
                statusBox.textContent = "Background loading stopped due to a network error.";
                running = false;
                toggleBtn.textContent = "Resume Recessive Check";
                showPageNotice("Gene background loading hit a network error.", "error");
                updateBatchWrapVisibility();
            }
        }

        toggleBtn.addEventListener("click", function () {
            running = !running;
            localStorage.setItem(pauseKey, running ? "0" : "1");
            setRunning(running);
        });

        setRunning(running);
        updateBatchWrapVisibility();
    })();
	
	function activateLoadingShell(shell) {
        if (!shell) return;
        shell.classList.add("is-loading");
    }

    document.querySelectorAll("[data-loading-shell-trigger]").forEach(function (trigger) {
        trigger.addEventListener("click", function () {
            const targetSelector = trigger.getAttribute("data-loading-shell-trigger");
            if (!targetSelector) return;

            const shell = document.querySelector(targetSelector);
            activateLoadingShell(shell);
        });
    });

    document.querySelectorAll("form[data-loading-shell-form]").forEach(function (form) {
        form.addEventListener("submit", function () {
            const targetSelector = form.getAttribute("data-loading-shell-form");
            if (!targetSelector) return;

            const shell = document.querySelector(targetSelector);
            activateLoadingShell(shell);

            const button = form.querySelector('button[type="submit"]');
            if (button) {
                button.disabled = true;
                button.classList.add("is-pending");
                button.textContent = button.getAttribute("data-loading-label") || "Analyzing...";
            }
        });
    });
	
});

document.addEventListener("DOMContentLoaded", function () {
    const searchInput = document.getElementById("ip-available-search");
    const searchButton = document.getElementById("ip-available-search-btn");
    const clearButton = document.getElementById("ip-available-search-clear-btn");
    const availableTable = document.querySelector(".ip-available-table table");

    if (!searchInput || !searchButton || !availableTable) {
        return;
    }

    const availableRows = Array.from(
        availableTable.querySelectorAll("tbody tr.clickable-row[data-token-id]")
    );

    const filterDropdowns = Array.from(
        document.querySelectorAll(".ip-column-filter-form .ip-filter-dropdown")
    );

    function normalizeSearchValue(value) {
        return String(value || "").trim().toLowerCase();
    }

	function hasActiveCheckboxOrRadioFilter(detailsEl) {
		if (!detailsEl) return false;
		return Array.from(
			detailsEl.querySelectorAll('input[type="checkbox"], input[type="radio"]')
		).some(function (input) {
			return input.checked;
		});
	}

	function hasActiveMinIpFilter(detailsEl) {
		if (!detailsEl) return false;

		const input = detailsEl.querySelector(
			'input[name="min_ip"], input[name="gene_min_ip"], input[name="ultimate_min_ip"]'
		);

		if (!input) return false;
		return String(input.value || "").trim() !== "";
	}
	
    function isFilterDropdownActive(detailsEl) {
        return hasActiveCheckboxOrRadioFilter(detailsEl) || hasActiveMinIpFilter(detailsEl);
    }
	
	function getFilterSummaryText(detailsEl) {
		if (!detailsEl) return "Filter";

		const minIpInput = detailsEl.querySelector(
			'input[name="min_ip"], input[name="gene_min_ip"], input[name="ultimate_min_ip"]'
		);
		if (minIpInput) {
			const value = String(minIpInput.value || "").trim();
			return value ? `Min ${value}` : "Filter";
		}

		const checkedCheckboxes = Array.from(
			detailsEl.querySelectorAll('.ip-filter-csv-source:checked')
		);
		if (checkedCheckboxes.length > 0) {
			if (checkedCheckboxes.length === 1) {
				const labelText = checkedCheckboxes[0].closest("label")?.innerText?.trim();
				return labelText || "1 selected";
			}
			return `${checkedCheckboxes.length} selected`;
		}

		const checkedRadio = detailsEl.querySelector('input[type="radio"]:checked');
		if (checkedRadio) {
			return "1 selected";
		}

		return "Filter";
	}

	function updateFilterSummary(detailsEl) {
		if (!detailsEl) return;

		const summary = detailsEl.querySelector(".ip-filter-summary");
		if (!summary) return;

		const text = getFilterSummaryText(detailsEl);
		summary.textContent = text;

		detailsEl.classList.toggle("is-active", isFilterDropdownActive(detailsEl));
	}

	function updateAllFilterSummaries() {
		filterDropdowns.forEach(function (dropdown) {
			updateFilterSummary(dropdown);
		});
	}

    function updateClearButtonVisibility() {
        const hasSearch = normalizeSearchValue(searchInput.value) !== "";
        if (clearButton) {
            clearButton.classList.toggle("hidden", !hasSearch);
        }
    }

    function applyAvailableSearch() {
        const searchValue = normalizeSearchValue(searchInput.value);

        let visibleCount = 0;

        availableRows.forEach(function (row) {
            const tokenId = normalizeSearchValue(row.getAttribute("data-token-id"));
            const isMatch = !searchValue || tokenId.indexOf(searchValue) !== -1;

            row.classList.toggle("hidden", !isMatch);

            if (isMatch) {
                visibleCount += 1;
            }
        });

        let noResultRow = availableTable.querySelector(".ip-search-empty-row");

        if (visibleCount === 0 && availableRows.length > 0) {
            if (!noResultRow) {
                noResultRow = document.createElement("tr");
                noResultRow.className = "ip-search-empty-row";
                noResultRow.innerHTML = `
                    <td colspan="6">
                        <div class="empty-state-card empty-state-card-inline">
                            <div class="empty-state-kicker">No search result</div>
                            <p>No chicken matched the current ID search.</p>
                        </div>
                    </td>
                `;
                availableTable.querySelector("tbody").appendChild(noResultRow);
            }
        } else if (noResultRow) {
            noResultRow.remove();
        }

        updateClearButtonVisibility();
    }

    function closeUnusedDropdowns(exceptDropdown) {
        filterDropdowns.forEach(function (dropdown) {
            if (dropdown === exceptDropdown) return;
            if (isFilterDropdownActive(dropdown)) return;
            dropdown.removeAttribute("open");
        });
    }

    searchButton.addEventListener("click", function () {
        applyAvailableSearch();
    });

	searchInput.addEventListener("keydown", function (event) {
		if (event.key === "Enter") {
			event.preventDefault();
			applyAvailableSearch();
			return;
		}

		if (event.key === "Escape") {
			searchInput.value = "";
			applyAvailableSearch();
			searchInput.blur();
		}
	});

    searchInput.addEventListener("input", function () {
        updateClearButtonVisibility();

        if (normalizeSearchValue(searchInput.value) === "") {
            applyAvailableSearch();
        }
    });

    if (clearButton) {
        clearButton.addEventListener("click", function () {
            searchInput.value = "";
            applyAvailableSearch();
            searchInput.focus();
        });
    }

    filterDropdowns.forEach(function (dropdown) {
        dropdown.addEventListener("toggle", function () {
            if (dropdown.open) {
                closeUnusedDropdowns(dropdown);
            }
        });
    });
	
	filterDropdowns.forEach(function (dropdown) {
		dropdown.querySelectorAll('input[type="checkbox"], input[type="radio"], input[type="number"]').forEach(function (input) {
			input.addEventListener("change", function () {
				updateFilterSummary(dropdown);
			});

			input.addEventListener("input", function () {
				updateFilterSummary(dropdown);
			});
		});
	});

    document.addEventListener("click", function (event) {
        const clickedInsideFilter = event.target.closest(".ip-column-filter-form .ip-filter-dropdown");
        if (clickedInsideFilter) return;

        filterDropdowns.forEach(function (dropdown) {
            if (isFilterDropdownActive(dropdown)) return;
            dropdown.removeAttribute("open");
        });
    });

    updateAllFilterSummaries();
	applyAvailableSearch();
});
