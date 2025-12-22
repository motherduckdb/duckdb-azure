#include "azure_dfs_filesystem.hpp"

#include "azure_storage_account_client.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/logging/file_system_logger.hpp"

#include <algorithm>
#include <azure/storage/common/storage_exception.hpp>
#include <azure/storage/files/datalake.hpp>
#include <azure/storage/files/datalake/datalake_directory_client.hpp>
#include <azure/storage/files/datalake/datalake_file_client.hpp>
#include <azure/storage/files/datalake/datalake_file_system_client.hpp>
#include <azure/storage/files/datalake/datalake_options.hpp>
#include <azure/storage/files/datalake/datalake_responses.hpp>
#include <cstddef>

namespace duckdb {
const string AzureDfsStorageFileSystem::SCHEME = "abfss";
const string AzureDfsStorageFileSystem::PATH_PREFIX = "abfss://";
const string AzureDfsStorageFileSystem::UNSECURE_SCHEME = "abfs";
const string AzureDfsStorageFileSystem::UNSECURE_PATH_PREFIX = "abfs://";

inline static bool IsDfsScheme(const string &fpath) {
	return fpath.rfind(AzureDfsStorageFileSystem::PATH_PREFIX, 0) == 0 ||
	       fpath.rfind(AzureDfsStorageFileSystem::UNSECURE_PATH_PREFIX, 0) == 0;
}

static void Walk(const Azure::Storage::Files::DataLake::DataLakeFileSystemClient &fs, const std::string &path,
                 const string &path_pattern, std::size_t end_match, std::vector<OpenFileInfo> *out_result) {
	auto directory_client = fs.GetDirectoryClient(path);

	bool recursive = false;
	const auto double_star = path_pattern.rfind("**", end_match);
	if (double_star != std::string::npos) {
		if (path_pattern.length() > end_match) {
			throw NotImplementedException("abfss do not manage recursive lookup patterns, %s is therefor illegal, only "
			                              "pattern ending by ** are allowed.",
			                              path_pattern);
		}
		// pattern end with a **, perform recursive listing from this point
		recursive = true;
	}

	Azure::Storage::Files::DataLake::ListPathsOptions options;
	while (true) {
		auto res = directory_client.ListPaths(recursive, options);

		for (const auto &elt : res.Paths) {
			if (elt.IsDirectory) {
				if (!recursive) { // Only perform recursive call if we are not already processing recursive result
					if (Glob(elt.Name.data(), elt.Name.length(), path_pattern.data(), end_match)) {
						if (end_match >= path_pattern.length()) {
							// Skip, no way there will be matches anymore
							continue;
						}
						Walk(fs, elt.Name, path_pattern,
						     std::min(path_pattern.length(), path_pattern.find('/', end_match + 1)), out_result);
					}
				}
			} else {
				// File
				if (Glob(elt.Name.data(), elt.Name.length(), path_pattern.data(), path_pattern.length())) {
					OpenFileInfo info(elt.Name);
					info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
					auto &options = info.extended_info->options;
					options.emplace("file_size", Value::BIGINT(elt.FileSize));
					options.emplace("last_modified",
					                Value::TIMESTAMP(AzureStorageFileSystem::ToTimestamp(elt.LastModified)));
					out_result->push_back(info);
				}
			}
		}

		if (res.NextPageToken) {
			options.ContinuationToken = res.NextPageToken;
		} else {
			break;
		}
	}
}

//////// AzureDfsContextState ////////
AzureDfsContextState::AzureDfsContextState(Azure::Storage::Files::DataLake::DataLakeServiceClient client,
                                           const AzureReadOptions &azure_read_options)
    : AzureContextState(azure_read_options), service_client(std::move(client)) {
}

Azure::Storage::Files::DataLake::DataLakeFileSystemClient
AzureDfsContextState::GetDfsFileSystemClient(const std::string &file_system_name) const {
	return service_client.GetFileSystemClient(file_system_name);
}

//////// AzureDfsContextState ////////
AzureDfsStorageFileHandle::AzureDfsStorageFileHandle(AzureDfsStorageFileSystem &fs, const OpenFileInfo &info,
                                                     FileOpenFlags flags, const AzureReadOptions &read_options,
                                                     Azure::Storage::Files::DataLake::DataLakeFileClient client)
    : AzureFileHandle(fs, info, flags, FileType::FILE_TYPE_INVALID, read_options), file_client(std::move(client)) {
}

//////// AzureDfsStorageFileSystem ////////
unique_ptr<AzureFileHandle> AzureDfsStorageFileSystem::CreateHandle(const OpenFileInfo &info, FileOpenFlags flags,
                                                                    optional_ptr<FileOpener> opener) {
	if (!opener) {
		throw InternalException("Unsupported(INTERNAL): cannot create an Azure file Handle without FileOpener");
	}
	if (flags.Compression() != FileCompressionType::UNCOMPRESSED) {
		throw InternalException("Unsupported(INTERNAL): cannot open an Azure file in compressed mode");
	}
	if (flags.OpenForWriting()) {
		throw NotImplementedException("Unsupported: cannot open an Azure file in write mode");
	}
	if (flags.OpenForAppending()) {
		throw NotImplementedException("Unsupported: cannot open an Azure file in append mode");
	}
	if (flags.OpenForReading() && (flags.OpenForWriting() || flags.OpenForAppending())) {
		throw NotImplementedException("Unsupported: cannot open an Azure file in read+write mode");
	}

	auto parsed_url = ParseUrl(info.path);
	auto storage_context = GetOrCreateStorageContext(opener, info.path, parsed_url);
	auto file_system_client = storage_context->As<AzureDfsContextState>().GetDfsFileSystemClient(parsed_url.container);

	auto handle = make_uniq<AzureDfsStorageFileHandle>(*this, info, flags, storage_context->read_options,
	                                                   file_system_client.GetFileClient(parsed_url.path));
	if (!handle->PostConstruct()) {
		return nullptr;
	}
	return std::move(handle);
}

bool AzureDfsStorageFileSystem::CanHandleFile(const string &fpath) {
	return IsDfsScheme(fpath);
}

bool AzureDfsStorageFileSystem::DirectoryExists(const string &dirname, optional_ptr<FileOpener> opener) {
	auto handle = OpenFile(dirname, FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS, opener);
	return handle && handle->Cast<AzureDfsStorageFileHandle>().GetType() == FileType::FILE_TYPE_DIR;
}

void AzureDfsStorageFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	throw NotImplementedException("Unsupported in Azure ADLSv2: CreateDirectory");
}

bool AzureDfsStorageFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	auto handle = OpenFile(filename, FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS, opener);
	return handle && handle->Cast<AzureDfsStorageFileHandle>().GetType() == FileType::FILE_TYPE_REGULAR;
}

vector<OpenFileInfo> AzureDfsStorageFileSystem::Glob(const string &path, FileOpener *opener) {
	if (opener == nullptr) {
		throw InternalException("Cannot do Azure storage Glob without FileOpener");
	}

	auto azure_url = ParseUrl(path);

	// If path does not contains any wildcard, we assume that an absolute path therefor nothing to do
	auto first_wildcard_pos = azure_url.path.find_first_of("*[\\");
	if (first_wildcard_pos == string::npos) {
		vector<OpenFileInfo> rv;
		if (FileExists(path, opener)) {
			rv.emplace_back(path);
		}
		return rv;
	}

	// The path contains wildcard try to list file with the minimum calls
	auto dfs_storage_service = ConnectToDfsStorageAccount(opener, path, azure_url);
	auto dfs_filesystem_client = dfs_storage_service.GetFileSystemClient(azure_url.container);

	auto index_root_dir = azure_url.path.rfind('/', first_wildcard_pos);
	if (index_root_dir == string::npos) {
		index_root_dir = 0;
	}
	auto shared_path = azure_url.path.substr(0, index_root_dir);

	std::vector<OpenFileInfo> result;
	Walk(dfs_filesystem_client, shared_path,
	     // pattern to match
	     azure_url.path, std::min(azure_url.path.length(), azure_url.path.find('/', index_root_dir + 1)),
	     // output result
	     &result);

	if (!result.empty()) {
		const auto path_result_prefix =
		    (azure_url.is_fully_qualified ? (azure_url.prefix + azure_url.storage_account_name + '.' +
		                                     azure_url.endpoint + '/' + azure_url.container)
		                                  : (azure_url.prefix + azure_url.container)) +
		    '/';
		for (auto &elt : result) {
			elt.path = path_result_prefix + elt.path;
		}
	}

	return result;
}

bool AzureDfsStorageFileSystem::ListFilesExtended(const string &path_in,
                                                  const std::function<void(OpenFileInfo &info)> &callback,
                                                  optional_ptr<FileOpener> opener) {
	if (path_in.find('*') != string::npos) {
		throw InvalidInputException("ListFiles does not support globs");
	}
	// Normalize: to end with '/' so it's a clear dir prefix
	auto url = ParseUrl(path_in);
	auto storage_context = GetOrCreateStorageContext(opener, path_in, url);
	auto fs = storage_context->As<AzureDfsContextState>().GetDfsFileSystemClient(url.container);
	auto dir_client = fs.GetDirectoryClient(url.path);

	auto child_strip_len = url.path.size() + (StringUtil::EndsWith(url.path, "/") ? 0 : 1);
	bool rv = false;
	for (auto page = dir_client.ListPaths(false); page.HasPage(); page.MoveToNextPage()) {
		for (auto &child : page.Paths) {
			rv = true;
			// Strangely, the DataLake API returns whole path, where the Blob API (correctly?)
			// only returns the child name without prefix.
			OpenFileInfo info(child.Name.substr(child_strip_len));
			info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
			auto &options = info.extended_info->options;
			Value file_type(child.IsDirectory ? "directory" : "file");
			options.emplace("type", std::move(file_type));
			options.emplace("file_size", Value::BIGINT(UnsafeNumericCast<int64_t>(child.FileSize)));
			options.emplace("last_modified", Value::TIMESTAMP(ToTimestamp(child.LastModified)));

			// NOTE: there's a LOT of metadata available, and tags, etc. -- see
			// https://github.com/Azure/azure-sdk-for-cpp/blob/main/sdk/storage/azure-storage-blobs/inc/azure/storage/blobs/rest_client.hpp#L1134
			// (struct BlobItemDetails) and
			// https://learn.microsoft.com/en-us/rest/api/storageservices/list-blobs
			callback(info);
		}
	}
	return rv;
}

void AzureDfsStorageFileSystem::LoadRemoteFileInfo(AzureFileHandle &handle) {
	auto &afh = handle.Cast<AzureDfsStorageFileHandle>();

	if (afh.IsRemoteLoaded()) {
		return;
	}

	auto res_props = afh.file_client.GetProperties();
	auto is_dir = res_props.Value.IsDirectory;
	afh.is_remote_loaded = true; // always set loaded
	afh.file_type = is_dir ? FileType::FILE_TYPE_DIR : FileType::FILE_TYPE_REGULAR;
	afh.length = is_dir ? 0 : res_props.Value.FileSize;
	afh.last_modified = ToTimestamp(res_props.Value.LastModified);
	afh.file_offset = 0; // always reset offset state
}

void AzureDfsStorageFileSystem::ReadRange(AzureFileHandle &handle, idx_t file_offset, char *buffer_out,
                                          idx_t buffer_out_len) {
	auto &afh = handle.Cast<AzureDfsStorageFileHandle>();
	try {
		// Specify the range
		Azure::Core::Http::HttpRange range;
		range.Offset = (int64_t)file_offset;
		range.Length = buffer_out_len;
		Azure::Storage::Files::DataLake::DownloadFileToOptions options;
		options.Range = range;
		options.TransferOptions.Concurrency = afh.read_options.transfer_concurrency;
		options.TransferOptions.InitialChunkSize = afh.read_options.transfer_chunk_size;
		options.TransferOptions.ChunkSize = afh.read_options.transfer_chunk_size;
		auto res = afh.file_client.DownloadTo((uint8_t *)buffer_out, buffer_out_len, options);

	} catch (const Azure::Storage::StorageException &e) {
		throw IOException("AzureBlobStorageFileSystem Read to '%s' failed with %s Reason Phrase: %s", afh.path,
		                  e.ErrorCode, e.ReasonPhrase);
	}
}

shared_ptr<AzureContextState> AzureDfsStorageFileSystem::CreateStorageContext(optional_ptr<FileOpener> opener,
                                                                              const string &path,
                                                                              const AzureParsedUrl &parsed_url) {
	auto azure_read_options = ParseAzureReadOptions(opener);

	return make_shared_ptr<AzureDfsContextState>(ConnectToDfsStorageAccount(opener, path, parsed_url),
	                                             azure_read_options);
}

} // namespace duckdb
