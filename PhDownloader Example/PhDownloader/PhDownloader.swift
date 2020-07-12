//
//  PhDownloader.swift
//  PhDownloader Example
//
//  Created by Petrus on 7/4/20.
//  Copyright © 2020 Petrus Nguyễn Thái Học. All rights reserved.
//

import Foundation
import RxSwift
import RxRelay
import RealmSwift
import RxRealm
import RxAlamofire
import Alamofire

// MARK: - PhDownloader

public protocol PhDownloader {
  func observeState(by identifier: String) -> Observable<PhDownloadState>

  func observeState<T: Sequence>(by identifiers: T) -> Observable<[String: PhDownloadState]> where T.Element == String

  var downloadResult$: Observable<PhDownloadResult> { get }

  func enqueue(_ request: PhDownloadRequest) -> Completable

  func cancel(by identifier: String) -> Completable

  func cancelAll() -> Completable
}

public enum PhDownloaderError: Error, CustomDebugStringConvertible {
  case destroyed
  case downloadError(Error)

  public var debugDescription: String {
    switch self {
    case .destroyed:
      return "Downloader already destroyed. Please keep strong reference to it to prevent the deallocation"
    case .downloadError(let error):
      return "Download failure: \(error)"
    }
  }
}

public typealias PhDownloadResult = Swift.Result<PhDownloadRequest, PhDownloaderError>

public enum PhDownloaderFactory {
  public static func downloader(with options: PhDownloaderOptions) throws -> PhDownloader {
    let dataSource = PhDownloadRealLocalDataSource(realm: try realm())
    return RealSwiftyDownloader(
      options: options,
      dataSource: dataSource
    )
  }
}

// MARK: - PhDownloadLocalDataSource

protocol PhDownloadLocalDataSource {
  func update(id: String, state: PhDownloadState) throws

  func insertOrUpdate(
    identifier: String,
    url: URL,
    fileName: String,
    savedDir: URL,
    state: PhDownloadState
  ) throws

  func getResults(by ids: Set<String>) -> Results<PhDownloadTaskEntity>

  func getResults(by id: String) -> Results<PhDownloadTaskEntity>

  func get(by id: String) -> PhDownloadTaskEntity?
}

extension FileManager {
  static func phDownloaderDirectory() throws -> URL {
    let url = FileManager.default
      .urls(for: .documentDirectory, in: .userDomainMask)
      .first!
      .appendingPathComponent("phDownloader", isDirectory: true)

    if !FileManager.default.fileExists(atPath: url.path) {
      try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
    }

    return url
  }
}

func realm() throws -> Realm {
  let fileURL = try FileManager.phDownloaderDirectory().appendingPathComponent("phdownloader_default.realm")
  print(fileURL)

  let configuration = Realm.Configuration(
    fileURL: fileURL,
    // Set the new schema version. This must be greater than the previously used
    // version (if you've never set a schema version before, the version is 0).
    schemaVersion: 1,

    // Set the block which will be called automatically when opening a Realm with
    // a schema version lower than the one set above
    migrationBlock: { migration, oldSchemaVersion in
      // We haven’t migrated anything yet, so oldSchemaVersion == 0
      if (oldSchemaVersion < 1) {
        // Nothing to do!
        // Realm will automatically detect new properties and removed properties
        // And will update the schema on disk automatically
      }
    }
  )

  return try Realm(configuration: configuration)
}

class PhDownloadRealLocalDataSource: PhDownloadLocalDataSource {
  private let realm: Realm

  init(realm: Realm) {
    self.realm = realm
  }

  func update(id: String, state: PhDownloadState) throws {
    guard let task = self.find(by: id) else {
      return
    }

    try self.realm.write {
      task.state = state.toInt
      task.updatedAt = .init()
    }
  }

  func insertOrUpdate(
    identifier: String,
    url: URL,
    fileName: String,
    savedDir: URL,
    state: PhDownloadState
  ) throws {
    try self.realm.write {
      self.realm.add(
        PhDownloadTaskEntity(
          identifier: identifier,
          url: url,
          fileName: fileName,
          savedDir: savedDir,
          state: state.toInt
        ),
        update: .modified
      )
    }
  }

  func getResults(by ids: Set<String>) -> Results<PhDownloadTaskEntity> {
    self.realm
      .objects(PhDownloadTaskEntity.self)
      .filter("SELF.identifier IN %@", ids)
  }

  func getResults(by id: String) -> Results<PhDownloadTaskEntity> {
    self.realm
      .objects(PhDownloadTaskEntity.self)
      .filter("SELF.identifier = %@", id)
  }

  func get(by id: String) -> PhDownloadTaskEntity? {
    self.find(by: id)
  }

  private func find(by id: String) -> PhDownloadTaskEntity? {
    self.realm.object(ofType: PhDownloadTaskEntity.self, forPrimaryKey: id)
  }
}

class PhDownloadTaskEntity: Object {
  @objc dynamic var identifier: String = ""
  @objc dynamic var url: String = ""
  @objc dynamic var fileName: String = ""
  @objc dynamic var savedDir: String = ""
  @objc dynamic var state: Int = undefined
  @objc dynamic var updatedAt: Date = .init()

  override class func primaryKey() -> String? { "identifier" }

  // MARK: Integer constants as download state

  static let undefined = -2
  static let enqueued = -1
  static let completed = 101
  static let failed = 102
  static let cancelled = 103

  convenience init(
    identifier: String,
    url: URL,
    fileName: String,
    savedDir: URL,
    state: Int
  ) {
    self.init()
    self.identifier = identifier
    self.url = url.absoluteString
    self.fileName = fileName
    self.savedDir = savedDir.path
    self.state = state
  }
}

extension PhDownloadState {
  var toInt: Int {
    switch self {
    case .undefined:
      return PhDownloadTaskEntity.undefined
    case .enqueued:
      return PhDownloadTaskEntity.enqueued
    case .downloading(let progress):
      return progress
    case .completed:
      return PhDownloadTaskEntity.completed
    case .failed:
      return PhDownloadTaskEntity.failed
    case .cancelled:
      return PhDownloadTaskEntity.cancelled
    }
  }
}

extension Int {
  var toDownloadState: PhDownloadState {
    switch self {
    case PhDownloadTaskEntity.undefined:
      return .undefined
    case PhDownloadTaskEntity.enqueued:
      return .enqueued
    case 0...100:
      return .downloading(progress: self)
    case PhDownloadTaskEntity.completed:
      return .completed
    case PhDownloadTaskEntity.failed:
      return .failed
    case PhDownloadTaskEntity.cancelled:
      return .cancelled
    default:
      fatalError("Invalid state: \(self)")
    }
  }
}

extension RxProgress {
  var asDownloadState: PhDownloadState {
    if self.totalBytes <= 0 { return .downloading(progress: 0) }
    let percent = Double(self.bytesWritten) / Double(self.totalBytes)
    return .downloading(progress: Int(100 * percent))
  }
}

extension Event where Element == RxProgress {
  var asDownloadState: PhDownloadState {
    switch self {
    case .next(let progress):
      return progress.asDownloadState
    case .error:
      return .failed
    case .completed:
      return .completed
    }
  }
}

/// The command that enqueues a download request or cancel by identifier
enum Command {
  case enqueue(request: PhDownloadRequest)
  case cancel(identifier: String)
}

// MARK: - RealSwiftyDownloader

final class RealSwiftyDownloader: PhDownloader {
  // MARK: Dependencies
  private let options: PhDownloaderOptions
  private let dataSource: PhDownloadLocalDataSource

  // MARK: ReactiveX
  private let commandS = PublishRelay<Command>()
  private let downloadResultS = PublishRelay<PhDownloadResult>()
  private let disposeBag = DisposeBag()

  // MARK: Schedulers
  private let throttleScheduler = SerialDispatchQueueScheduler(qos: .utility)
  private static var mainScheduler: MainScheduler { .instance }
  private static var concurrentMainScheduler: ConcurrentMainScheduler { .instance }

  internal init(options: PhDownloaderOptions, dataSource: PhDownloadLocalDataSource) {
    self.options = options
    self.dataSource = dataSource

    self
      .commandS
      .compactMap {
        if case .enqueue(request: let request) = $0 { return request }
        return nil
      }
      .map { [weak self] request in self?.createDownloadRequest(request) ?? .empty() }
      .merge(maxConcurrent: options.maxConcurrent)
      .subscribe()
      .disposed(by: self.disposeBag)
  }

  // MARK: Private helpers

  private func createDownloadRequest(_ request: PhDownloadRequest) -> Observable<Void>
  {
    Observable
      .deferred { [dataSource] () -> Observable<DownloadRequest> in
        // already cancelled before
        if dataSource.get(by: request.identifier)?.state.toDownloadState == .cancelled {
          return .empty()
        }

        let urlRequest = URLRequest(url: request.url)
        let destination: DownloadRequest.Destination = { (temporaryURL, response) in
          (
            request.savedDir.appendingPathComponent(request.fileName),
            [.createIntermediateDirectories, .removePreviousFile]
          )
        }
        return RxAlamofire.download(urlRequest, to: destination)
      }
      .flatMap { $0.rx.progress() }
      .takeUntil(self.cancelCommand(for: request.identifier))
      .throttle(self.options.throttleProgress, latest: true, scheduler: self.throttleScheduler)
      .distinctUntilChanged()
      .materialize()
      .observeOn(Self.mainScheduler)
      .do(
        onNext: { [downloadResultS, dataSource] notification in

          try? dataSource.update(
            id: request.identifier,
            state: notification.asDownloadState
          )

          switch notification {
          case .error(let error):
            downloadResultS.accept(.failure(.downloadError(error)))
          case .completed:
            downloadResultS.accept(.success(request))
          case .next:
            ()
          }
        }
      )
      .map { _ in () }
      .catchError { error in
        print("downloadProgress error: \(error)")
        return .empty()
    }
  }

  private func cancelCommand(for identifierNeedCancel: String) -> Observable<String> {
    self.commandS.compactMap {
      if case .cancel(let identifier) = $0, identifier == identifierNeedCancel { return identifier }
      return nil
    }
  }

  private func cancelDownload(_ identifier: String) {
    DispatchQueue.main.async {
      if let task = self.dataSource.get(by: identifier) {
        let fileURL = URL(fileURLWithPath: task.savedDir).appendingPathComponent(task.fileName)
        try? FileManager.default.removeItem(at: fileURL)
      }
      try? self.dataSource.update(id: identifier, state: .cancelled)
    }
  }
}

// MARK: Observe state by identifier
extension RealSwiftyDownloader {
  func observeState(by identifier: String) -> Observable<PhDownloadState> {
    Observable
      .deferred { [weak self] () -> Observable<PhDownloadState> in
        guard let self = self else {
          return .error(PhDownloaderError.destroyed)
        }

        if let task = self.dataSource.get(by: identifier) {
          return Observable
            .from(object: task, emitInitialValue: true)
            .map { $0.state.toDownloadState }
            .subscribeOn(Self.concurrentMainScheduler)
        }

        return Observable
          .collection(
            from: self.dataSource.getResults(by: identifier),
            synchronousStart: true,
            on: .main
          )
          .map { results in results.first?.state.toDownloadState ?? .undefined }
          .subscribeOn(Self.concurrentMainScheduler)
      }
      .distinctUntilChanged()
  }

}

// MARK: Observe state by identifiers
extension RealSwiftyDownloader {
  func observeState<T: Sequence>(by identifiers: T) -> Observable<[String: PhDownloadState]> where T.Element == String {
    Observable
      .collection(
        from: self.dataSource.getResults(by: Set(identifiers)),
        synchronousStart: true,
        on: .main
      )
      .map { results in
        let tuples = Array(results).map { ($0.identifier, $0.state.toDownloadState) }
        let stateById = Dictionary(uniqueKeysWithValues: tuples)
        return Dictionary(uniqueKeysWithValues: identifiers.map { ($0, stateById[$0] ?? .undefined) })
      }
      .distinctUntilChanged()
  }
}

// MARK: Download result observable
extension RealSwiftyDownloader {
  var downloadResult$: Observable<PhDownloadResult> { self.downloadResultS.asObservable() }
}

// MARK: Enqueue download request
extension RealSwiftyDownloader {
  func enqueue(_ request: PhDownloadRequest) -> Completable {
    Completable
      .deferred { [weak self] () -> Completable in
        guard let self = self else {
          return .error(PhDownloaderError.destroyed)
        }

        do {
          try self.dataSource.insertOrUpdate(
            identifier: request.identifier,
            url: request.url,
            fileName: request.fileName,
            savedDir: request.savedDir,
            state: .enqueued
          )
        } catch {
          return .error(error)
        }

        self.commandS.accept(.enqueue(request: request))
        return .empty()
      }
      .subscribeOn(Self.concurrentMainScheduler)
  }
}

// MARK: Cancel by identifier
extension RealSwiftyDownloader {
  func cancel(by identifier: String) -> Completable {
    Completable
      .deferred { [weak self] () -> Completable in
        guard let self = self else {
          return .error(PhDownloaderError.destroyed)
        }

        // mask task as cancelled
        // to prevent executing enqueued task
        self.cancelDownload(identifier)

        // send command to cancel downloading task
        self.commandS.accept(.cancel(identifier: identifier))

        return .empty()
      }
      .subscribeOn(Self.concurrentMainScheduler)
  }
}

// MARK: Cancel all
extension RealSwiftyDownloader {
  func cancelAll() -> Completable {
    fatalError()
  }
}
