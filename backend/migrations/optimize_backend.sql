-- SECTION 5: Index Strategy
-- Filter by State
CREATE INDEX IF NOT EXISTS idx_state ON raw_google_map_drive_data (state);
-- Filter by Category
CREATE INDEX IF NOT EXISTS idx_category ON raw_google_map_drive_data (category);
-- Filter by City
CREATE INDEX IF NOT EXISTS idx_city ON raw_google_map_drive_data (city);
-- Filter by Phone (Search/Lookup)
CREATE INDEX IF NOT EXISTS idx_phone ON raw_google_map_drive_data (phone_number);
-- Most Common Dashboard Filter (State + Category drill-down)
CREATE INDEX IF NOT EXISTS idx_state_category ON raw_google_map_drive_data (state, category);

-- SECTION 9: drive_folder_registry Status Fix
-- Check if column needs modification (This command might fail if column is already enum, but it's safe to run)
ALTER TABLE drive_folder_registry 
MODIFY COLUMN status ENUM('PENDING', 'SCANNING', 'UPDATED', 'ERROR') DEFAULT 'PENDING';

-- SECTION 8: File Registry Consolidation
-- Migrate data from gdrive_sync_inventory to file_registry
INSERT IGNORE INTO file_registry (drive_file_id, filename, processed_at, status)
SELECT file_id, file_name, synced_at, 'PROCESSED' FROM gdrive_sync_inventory;

-- Drop the old table
DROP TABLE IF EXISTS gdrive_sync_inventory;
