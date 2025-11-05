"use strict";

// State management
const state = {
    query: new URL(location).searchParams.get("query") || "",
    epoch: Math.floor(Date.now() / 1000),
    orderBy: null,
    ascending: true,
    lastOrderedValue: null,
    lastID: null,
    totalResults: 0,
    isLoading: false
};

// Initialize on page load
window.onload = function() {
    initializeUI();
    setupEventListeners();
    
    // Set initial order
    if (state.query !== "") {
        setOrderBy("RELEVANCE");
    } else {
        state.ascending = false;
        setOrderBy("DISCOVERED_ON");
    }
    
    updateTitle();
    updateFeedLink();
    load();
};

function initializeUI() {
    const input = document.querySelector(".search-input");
    if (state.query) {
        input.value = state.query;
    }
    
    // Set initial sort direction UI
    const sortToggle = document.getElementById("sort-toggle");
    if (!state.ascending) {
        sortToggle.classList.add("descending");
    }
}

function setupEventListeners() {
    // Filter button clicks
    const filterButtons = document.querySelectorAll(".filter-btn");
    filterButtons.forEach(btn => {
        btn.addEventListener("click", function() {
            const order = this.getAttribute("data-order");
            
            // Disable relevance if no query
            if (order === "RELEVANCE" && !state.query) {
                return;
            }
            
            setOrderBy(order);
            resetAndReload();
        });
    });
    
    // Sort direction toggle
    const sortToggle = document.getElementById("sort-toggle");
    sortToggle.addEventListener("click", function() {
        state.ascending = !state.ascending;
        this.classList.toggle("descending");
        resetAndReload();
    });
    
    // Load more button
    const loadMoreBtn = document.getElementById("load-more-btn");
    loadMoreBtn.addEventListener("click", function() {
        load();
    });
    
    // Update relevance button state based on query
    updateRelevanceButtonState();
}

function updateRelevanceButtonState() {
    const relevanceBtn = document.querySelector('[data-order="RELEVANCE"]');
    if (!state.query) {
        relevanceBtn.disabled = true;
        relevanceBtn.style.opacity = "0.5";
        relevanceBtn.style.cursor = "not-allowed";
        relevanceBtn.title = "Enter a search query to sort by relevance";
    } else {
        relevanceBtn.disabled = false;
        relevanceBtn.style.opacity = "1";
        relevanceBtn.style.cursor = "pointer";
        relevanceBtn.title = "Sort by relevance";
    }
}

function setOrderBy(order) {
    const validValues = [
        "TOTAL_SIZE",
        "DISCOVERED_ON",
        "UPDATED_ON",
        "N_FILES",
        "N_SEEDERS",
        "N_LEECHERS",
        "RELEVANCE"
    ];
    
    if (!validValues.includes(order)) {
        console.error("Invalid order value:", order);
        return;
    }
    
    state.orderBy = order;
    
    // Update UI to show active filter
    const filterButtons = document.querySelectorAll(".filter-btn");
    filterButtons.forEach(btn => {
        if (btn.getAttribute("data-order") === order) {
            btn.classList.add("active");
        } else {
            btn.classList.remove("active");
        }
    });
}

function orderedValue(torrent) {
    switch (state.orderBy) {
        case "TOTAL_SIZE":
            return torrent.size;
        case "DISCOVERED_ON":
            return torrent.discoveredOn;
        case "N_FILES":
            return torrent.nFiles;
        case "RELEVANCE":
            return torrent.relevance;
        default:
            return 0;
    }
}

function resetAndReload() {
    // Reset pagination
    state.lastOrderedValue = null;
    state.lastID = null;
    state.totalResults = 0;
    
    // Clear results
    const container = document.getElementById("results-container");
    container.innerHTML = "";
    
    // Load new results
    load();
}

function updateTitle() {
    const title = document.querySelector("title");
    if (state.query) {
        title.textContent = state.query + " - magneticow";
    } else {
        title.textContent = "Most recent torrents - magneticow";
    }
}

function updateFeedLink() {
    const feedAnchor = document.getElementById("feed-anchor");
    if (state.query) {
        feedAnchor.setAttribute("href", "/feed?query=" + encodeURIComponent(state.query));
    } else {
        feedAnchor.setAttribute("href", "/feed");
    }
}

function updateResultsInfo() {
    const resultsCount = document.getElementById("results-count");
    if (state.totalResults === 0) {
        resultsCount.textContent = "No results found";
    } else if (state.totalResults === 1) {
        resultsCount.textContent = "1 result";
    } else {
        resultsCount.textContent = state.totalResults + " results";
    }
}

function load() {
    if (state.isLoading) return;
    
    state.isLoading = true;
    const button = document.getElementById("load-more-btn");
    const buttonText = button.querySelector("span");
    const container = document.getElementById("results-container");
    const template = document.getElementById("item-template").innerHTML;
    
    // Update button state
    button.disabled = true;
    buttonText.textContent = "Loading...";
    
    // Show loading state if first load
    if (state.totalResults === 0) {
        container.innerHTML = '<div class="loading"></div>';
    }
    
    const reqURL = "/api/v0.1/torrents?" + encodeQueryData({
        query: state.query,
        epoch: state.epoch,
        lastID: state.lastID,
        lastOrderedValue: state.lastOrderedValue,
        orderBy: state.orderBy,
        ascending: state.ascending
    });
    
    console.log("Loading torrents:", reqURL);
    
    fetch(reqURL)
        .then(response => {
            if (!response.ok) {
                throw new Error("Network response was not ok");
            }
            return response.json();
        })
        .then(torrents => {
            state.isLoading = false;
            
            // Remove loading state
            if (state.totalResults === 0) {
                container.innerHTML = "";
            }
            
            if (torrents.length === 0) {
                if (state.totalResults === 0) {
                    // No results at all
                    container.innerHTML = `
                        <div class="empty-state">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <circle cx="11" cy="11" r="8"></circle>
                                <path d="m21 21-4.35-4.35"></path>
                            </svg>
                            <h3>No torrents found</h3>
                            <p>Try adjusting your search query or filters</p>
                        </div>
                    `;
                } else {
                    // No more results
                    buttonText.textContent = "No More Results";
                }
                updateResultsInfo();
                return;
            }
            
            // Update pagination state
            const last = torrents[torrents.length - 1];
            state.lastID = last.id;
            state.lastOrderedValue = orderedValue(last);
            state.totalResults += torrents.length;
            
            // Render torrents
            torrents.forEach(torrent => {
                const rendered = renderTorrent(torrent);
                container.innerHTML += rendered;
            });
            
            // Update UI
            button.disabled = false;
            buttonText.textContent = "Load More Results";
            updateResultsInfo();
        })
        .catch(error => {
            state.isLoading = false;
            console.error("Error loading torrents:", error);
            
            if (state.totalResults === 0) {
                container.innerHTML = `
                    <div class="empty-state">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <circle cx="12" cy="12" r="10"></circle>
                            <line x1="12" y1="8" x2="12" y2="12"></line>
                            <line x1="12" y1="16" x2="12.01" y2="16"></line>
                        </svg>
                        <h3>Error loading torrents</h3>
                        <p>Please try again later</p>
                    </div>
                `;
            }
            
            button.disabled = false;
            buttonText.textContent = "Load More Results";
        });
}

function renderTorrent(torrent) {
    // Format data for display
    const data = {
        infoHash: torrent.infoHash,
        name: torrent.name,
        size: fileSize(torrent.size),
        nFiles: torrent.nFiles,
        nFilesText: torrent.nFiles === 1 ? "file" : "files",
        discoveredOn: humaniseDate(torrent.discoveredOn)
    };
    
    // Add relevance score if available (only when searching)
    if (state.query && torrent.relevance !== undefined) {
        // BM25 scores are negative, so we normalize them for display
        // Convert to a percentage-like score (higher is better)
        const normalizedScore = Math.max(0, Math.min(100, 100 + torrent.relevance * 10));
        data.relevance = true;
        data.relevanceScore = Math.round(normalizedScore);
    }
    
    const template = document.getElementById("item-template").innerHTML;
    return Mustache.render(template, data);
}
