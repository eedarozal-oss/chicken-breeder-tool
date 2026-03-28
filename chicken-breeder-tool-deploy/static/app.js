document.addEventListener("DOMContentLoaded", function () {
    const detailButtons = document.querySelectorAll(".toggle-details-btn");
    const compareModal = document.getElementById("compare-modal");
    const compareModalBody = document.getElementById("compare-modal-body");
    const compareOpenButtons = document.querySelectorAll(".open-compare-btn");
    const compareCloseButtons = document.querySelectorAll("[data-close-compare]");

    detailButtons.forEach((button) => {
        const openLabel = button.getAttribute("data-open-label") || button.textContent.trim();

        button.addEventListener("click", function () {
            const targetId = button.getAttribute("data-target");
            const detailsRow = document.getElementById(targetId);

            if (!detailsRow) return;

            const isHidden = detailsRow.classList.contains("hidden");

            document.querySelectorAll(".details-row").forEach((row) => {
                row.classList.add("hidden");
            });

            document.querySelectorAll(".toggle-details-btn").forEach((btn) => {
                const btnOpenLabel = btn.getAttribute("data-open-label") || "View Details";
                btn.textContent = btnOpenLabel;
            });

            if (isHidden) {
                detailsRow.classList.remove("hidden");
                button.textContent = "Hide";
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

    document.addEventListener("keydown", function (event) {
        if (event.key === "Escape") {
            closeCompareModal();
        }
    });

    // Auto-open the first match popup when auto_match=1 is present in the URL
    const urlParams = new URLSearchParams(window.location.search);
    const shouldAutoMatch = (urlParams.get("auto_match") || "").trim().toLowerCase();

    if (["1", "true", "yes", "on"].includes(shouldAutoMatch)) {
        const firstCompareButton = document.querySelector(".open-compare-btn");

        if (firstCompareButton) {
            const templateId = firstCompareButton.getAttribute("data-template-id");
            if (templateId) {
                openCompareModal(templateId);
            }
        }
    }
});