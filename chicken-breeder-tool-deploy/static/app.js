document.addEventListener("DOMContentLoaded", function () {
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

    const plannerModal = document.getElementById("planner-modal");
    const plannerOpenButtons = document.querySelectorAll(".open-planner-btn");
    const plannerCloseButtons = document.querySelectorAll("[data-close-planner]");

	let plannerStateChanged = false;
	let plannerRemovalChanged = false;

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
		url.searchParams.delete("popup_min_build_count");
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
			closeAutoMatchResultModal(false);
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
        const countWrap =
            document.querySelector("[data-auto-match-count-wrap]") ||
            document.querySelector(".multi-match-count-wrap");

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

        const isMultiple = selectedMode === "multiple";

        if (countWrap) countWrap.classList.toggle("hidden", !isMultiple);
        if (countInput) countInput.disabled = !isMultiple;
    }

    document.querySelectorAll('input[name="auto_match_mode"], select[name="auto_match_mode"]').forEach((input) => {
        input.addEventListener("change", syncAutoMatchConfigState);
    });

    syncAutoMatchConfigState();

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

		url.hash = "selected-chicken";
		window.location.href = url.toString();
	}

    async function submitPlannerFormAjax(form, button, mode) {
        if (!form || !button || button.disabled || form.dataset.submitting === "1") return;

        form.dataset.submitting = "1";
        button.disabled = true;
        button.classList.add("is-pending");

        try {
            const response = await fetch(form.action, {
                method: (form.method || "POST").toUpperCase(),
                body: new FormData(form),
                headers: {
                    "X-Requested-With": "XMLHttpRequest"
                },
                credentials: "same-origin",
                redirect: "follow"
            });

            if (!response.ok) {
                throw new Error("Request failed");
            }

			if (mode === "add") {
				markPlannerButtonAdded(button);
				updatePlannerCount(1);
				updateAvailablePairMax(-1);
				clearPlannerEmptyState();

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
			closeAutoMatchResultModal(false);
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