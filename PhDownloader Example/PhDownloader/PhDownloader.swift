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
  /// Observe state of download task by id
  /// - Parameter identifier: request id
  /// - Returns: an `Observable` that emits nil if task does not exist, otherwise it will emit the download task.
  func observe(by identifier: String) -> Observable<PhDownloadTask?>

  /// Observe state of download tasks by multiple ids
  /// - Parameter identifiers: request ids
  /// - Returns: an `Observable` that emits a dictionary with id as key, download task as value
  func observe<T: Sequence>(by identifiers: T) -> Observable<[String: PhDownloadTask]> where T.Element == String

  /// Download result event observable
  /// # Reference:
  /// [PhDownloadResult](x-source-tag://PhDownloadResult)
  var downloadResult$: Observable<PhDownloadResult> { get }

  /// Enqueue a download request
  func enqueue(_ request: PhDownloadRequest) -> Completable

  /// Cancel enqueued and running download task by identifier
  func cancel(by identifier: String) -> Completable

  /// Cancel all enqueued and running download tasks
  func cancelAll() -> Completable
}

/// It represents downloader errors
public enum PhDownloaderError: Error, CustomDebugStringConvertible {
  /// Realm error
  case databaseError(Error)

  /// Download error: No internet connection, file writing error, ...
  case downloadError(Error)

  /// Not found download task by identifier.
  case notFound(identifier: String)

  /// Task cannot be cancelled
  case cannotCancel(identifier: String)

  public var debugDescription: String {
    switch self {
    case .downloadError(let error):
      return "Download failure: \(error)."
    case .databaseError(let error):
      return "Database error: \(error)."
    case .notFound(let identifier):
      return "Not found task with identifier: \(identifier)."
    case .cannotCancel(let identifier):
      return "Cannot cancel task with identifier: \(identifier). Because state of task is finish (completed, failed or cancelled) or undefined"
    }
  }
}

/// It represents downloader result in three cases: `success`, `cancelled`, `failure`
/// - Tag: PhDownloadResult
public enum PhDownloadResult {
  case success(PhDownloadRequest)
  case cancelled(PhDownloadRequest)
  case failure(PhDownloadRequest, PhDownloaderError)
}

/// Provide `PhDownloader` from `PhDownloaderOptions`
public enum PhDownloaderFactory {

  /// Provide `PhDownloader` from `PhDownloaderOptions`
  public static func makeDownloader(with options: PhDownloaderOptions) -> PhDownloader {
    let queue = OperationQueue()
    queue.maxConcurrentOperationCount = options.maxConcurrent * 2

    let dataSource = RealLocalDataSource(
      realmInitializer: provideRealm,
      queue: queue
    )

    return RealDownloader(
      options: options,
      dataSource: dataSource
    )
  }
}

// MARK: - LocalDataSource

protocol LocalDataSource {

  /// Update download state for download task
  func update(id: String, state: PhDownloadState) -> Completable

  /// Insert or update download task
  func insertOrUpdate(
    identifier: String,
    url: URL,
    fileName: String,
    savedDir: URL,
    state: PhDownloadState
  ) -> Completable

  /// Get `Results` by multiple ids
  func getResults(by ids: Set<String>) throws -> Results<DownloadTaskEntity>

  /// Get `Results` by single id
  func getResults(by id: String) throws -> Results<DownloadTaskEntity>

  /// Get single task by id
  func get(by id: String) throws -> DownloadTaskEntity?

  /// Cancel all enqueued or running tasks
  /// - Returns: Cancelled task ids
  func cancelAll() -> Single<[String]>
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

func provideRealm() throws -> Realm {
  let fileURL = try FileManager.phDownloaderDirectory().appendingPathComponent("phdownloader_default.realm")

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

public protocol RealmAdapter {
  func objects<Element: Object>(_ type: Element.Type) -> Results<Element>

  func write<Result>(
    withoutNotifying tokens: [NotificationToken],
    _ block: () throws -> Result
  ) throws -> Result

  func add(_ object: Object, update: Realm.UpdatePolicy)

  func delete<Element: Object>(_ objects: Results<Element>)

  func refresh() -> Bool

  func object<Element: Object, KeyType>(ofType type: Element.Type, forPrimaryKey key: KeyType) -> Element?
}

extension Realm: RealmAdapter { }

final class RealLocalDataSource: LocalDataSource {

  /// Since we use `Realm` in background thread
  private let realmInitializer: () throws -> RealmAdapter

  /// OperationQueue that is used to dispatch blocks that updated realm `Object`
  private let queue: OperationQueue

  /// Scheduler that schedule query works
  private let queryScheduler = ConcurrentDispatchQueueScheduler(
    queue: .init(
      label: "RealLocalDataSource.QueryQueue",
      qos: .userInitiated,
      attributes: .concurrent
    )
  )

  init(realmInitializer: @escaping () throws -> RealmAdapter, queue: OperationQueue) {
    self.realmInitializer = realmInitializer
    self.queue = queue
  }

  func update(id: String, state: PhDownloadState) -> Completable {
      .create { [queue, realmInitializer] obsever -> Disposable in
        let disposable = BooleanDisposable()

        queue.addOperation {
          autoreleasepool {
            do {
              if disposable.isDisposed { return }

              let realm = try realmInitializer()
              _ = realm.refresh()

              guard let task = Self.find(by: id, in: realm) else {
                let error = PhDownloaderError.notFound(identifier: id)
                return obsever(.error(error))
              }

              if disposable.isDisposed { return }

              try realm.write(withoutNotifying: []) {
                task.update(to: state)
                task.updatedAt = .init()
              }

              obsever(.completed)
            } catch {
              obsever(.error(error))
            }
          }
        }

        return disposable
    }
  }

  func insertOrUpdate(
    identifier: String,
    url: URL,
    fileName: String,
    savedDir: URL,
    state: PhDownloadState
  ) -> Completable {
      .create { [queue, realmInitializer] observer -> Disposable in
        let disposable = BooleanDisposable()

        queue.addOperation {
          autoreleasepool {
            do {
              if disposable.isDisposed { return }

              let realm = try realmInitializer()

              if disposable.isDisposed { return }

              try realm.write(withoutNotifying: []) {
                realm.add(
                  DownloadTaskEntity(
                    identifier: identifier,
                    url: url,
                    fileName: fileName,
                    savedDir: savedDir,
                    state: state,
                    updatedAt: .init()
                  ),
                  update: .modified
                )
              }

              observer(.completed)
            } catch {
              observer(.error(error))
            }
          }
        }

        return disposable
    }
  }

  func getResults(by ids: Set<String>) throws -> Results<DownloadTaskEntity> {
    try self.realmInitializer()
      .objects(DownloadTaskEntity.self)
      .filter("SELF.identifier IN %@", ids)
  }

  func getResults(by id: String) throws -> Results<DownloadTaskEntity> {
    try self.realmInitializer()
      .objects(DownloadTaskEntity.self)
      .filter("SELF.identifier = %@", id)
  }

  func get(by id: String) throws -> DownloadTaskEntity? {
    let realm = try self.realmInitializer()
    _ = realm.refresh()
    return Self.find(by: id, in: realm)
  }

  func cancelAll() -> Single<[String]> {
    Single
      .deferred { () -> Single<[String]> in
        autoreleasepool {
          do {
            let realm = try self.realmInitializer()
            _ = realm.refresh()

            let entities = realm
              .objects(DownloadTaskEntity.self)
              .filter(
                "SELF.state = %@ OR SELF.state = %@",
                DownloadTaskEntity.RawState.enqueued.rawValue,
                DownloadTaskEntity.RawState.downloading.rawValue
              )
              .toArray()

            try realm.write(withoutNotifying: []) {
              entities.forEach { entity in
                entity.update(to: .cancelled)
                entity.updatedAt = .init()
              }
            }

            return .just(entities.map { $0.identifier })
          } catch {
            return .error(PhDownloaderError.databaseError(error))
          }
        }
      }
      .subscribeOn(self.queryScheduler)
  }

  private static func find(by id: String, in realm: RealmAdapter) -> DownloadTaskEntity? {
    realm.object(ofType: DownloadTaskEntity.self, forPrimaryKey: id)
  }
}

final class DownloadTaskEntity: Object {
  @objc dynamic var identifier: String = ""
  @objc dynamic var url: String = ""
  @objc dynamic var fileName: String = ""
  @objc dynamic var savedDir: String = ""
  @objc dynamic var updatedAt: Date = .init()

  @objc private dynamic var state: RawState = .undefined
  private dynamic var bytesWritten = RealmOptional<Int64>()
  private dynamic var totalBytes = RealmOptional<Int64>()

  override class func primaryKey() -> String? { "identifier" }

  @objc enum RawState: Int, RealmEnum {
    case undefined
    case enqueued
    case downloading
    case completed
    case failed
    case cancelled

    init(from phDownloadState: PhDownloadState) {
      switch phDownloadState {
      case .undefined:
        self = .undefined
      case .enqueued:
        self = .enqueued
      case .downloading:
        self = .downloading
      case .completed:
        self = .completed
      case .failed:
        self = .failed
      case .cancelled:
        self = .cancelled
      }
    }
  }

  /// Must be in transaction
  convenience init(
    identifier: String,
    url: URL,
    fileName: String,
    savedDir: URL,
    state: PhDownloadState,
    updatedAt: Date
  ) {
    self.init()
    self.identifier = identifier
    self.url = url.absoluteString
    self.fileName = fileName
    self.savedDir = savedDir.path
    self.updatedAt = updatedAt
    self.update(to: state)
  }

  /// Must be in transaction
  func update(to state: PhDownloadState) {
    self.state = .init(from: state)
    if case .downloading(let bytesWritten, let totalBytes, _) = state {
      self.bytesWritten.value = bytesWritten
      self.totalBytes.value = totalBytes
    }
  }

  var phDownloadState: PhDownloadState {
    switch state {
    case .undefined:
      return .undefined
    case .enqueued:
      return .enqueued
    case .downloading:
      let bytesWritten = self.bytesWritten.value!
      let totalBytes = self.totalBytes.value!

      return .downloading(
        bytesWritten: bytesWritten,
        totalBytes: totalBytes,
        percentage: percentage(bytesWritten: bytesWritten, totalBytes: totalBytes)
      )
    case .completed:
      return .completed
    case .failed:
      return .completed
    case .cancelled:
      return .cancelled
    }
  }
}

// MARK: - Utils

func percentage(bytesWritten: Int64, totalBytes: Int64) -> Int {
  guard totalBytes > 0 else { return 0 }
  let percent = Double(bytesWritten) / Double(totalBytes)
  return Int(100 * percent)
}

extension Event where Element == RxProgress {
  func asDownloadState(_ isCompleted: Bool) -> PhDownloadState {
    switch self {
    case .next(let progress):
      let bytesWritten = progress.bytesWritten
      let totalBytes = progress.totalBytes

      return .downloading(
        bytesWritten: bytesWritten,
        totalBytes: totalBytes,
        percentage: percentage(
          bytesWritten: bytesWritten,
          totalBytes: totalBytes
        )
      )
    case .error:
      return .failed
    case .completed:
      return isCompleted ? .completed : .cancelled
    }
  }
}

// MARK: PhDownloadTask + Initializer
extension PhDownloadTask {
  init(from entity: DownloadTaskEntity) {
    self.init(
      request: .init(
        identifier: entity.identifier,
        url: URL(string: entity.url)!,
        fileName: entity.fileName,
        savedDir: URL(fileURLWithPath: entity.savedDir)
      ),
      state: entity.phDownloadState
    )
  }
}

// MARK: DownloadTaskEntity + CanCancel + CanDownload
extension DownloadTaskEntity {
  /// Enqueued or runnning state
  var canCancel: Bool {
    if self.phDownloadState == .enqueued { return true }
    if case .downloading = self.phDownloadState { return true }
    return false
  }

  var canDownload: Bool { self.phDownloadState != .cancelled }
}

/// The command that enqueues a download request or cancel by identifier
enum Command {
  case enqueue(request: PhDownloadRequest)
  case cancel(identifier: String)
}

// MARK: - RealDownloader

final class RealDownloader: PhDownloader {
  // MARK: Dependencies
  private let options: PhDownloaderOptions
  private let dataSource: LocalDataSource

  // MARK: ReactiveX
  private let commandS = PublishRelay<Command>()
  private let downloadResultS = PublishRelay<PhDownloadResult>()
  private let disposeBag = DisposeBag()

  // MARK: Schedulers
  private let throttleScheduler = SerialDispatchQueueScheduler(qos: .utility)
  private static var mainScheduler: MainScheduler { .instance }
  private static var concurrentMainScheduler: ConcurrentMainScheduler { .instance }

  internal init(options: PhDownloaderOptions, dataSource: LocalDataSource) {
    self.options = options
    self.dataSource = dataSource

    self
      .commandS
      .compactMap {
        if case .enqueue(request: let request) = $0 { return request }
        return nil
      }
      .map { [weak self] request in self?.executeDownload(request) ?? .empty() }
      .merge(maxConcurrent: options.maxConcurrent)
      .subscribe()
      .disposed(by: self.disposeBag)
  }

  // MARK: Private helpers

  /// Execute download request, update local database and send result.
  /// All errors is sent to `downloadResultS` and  is discarded afterwards.
  /// - Parameter request: Download request
  /// - Returns: a Completable that always completed
  private func executeDownload(_ request: PhDownloadRequest) -> Completable {
    Completable
      .deferred { [downloadResultS, dataSource] () -> Completable in

        // check already cancelled before
        guard let task = try dataSource.get(by: request.identifier), task.canDownload else {
          print("[PhDownloader] Task with identifier: \(request.identifier) does not exist or cancelled")
          return .empty()
        }

        let urlRequest = URLRequest(url: request.url)
        let destination: DownloadRequest.Destination = { (temporaryURL, response) in
          (
            request.savedDir.appendingPathComponent(request.fileName),
            [.createIntermediateDirectories, .removePreviousFile]
          )
        }

        // Is task completed naturally
        var isCompleted = false

        return RxAlamofire
          .download(urlRequest, to: destination)
          .flatMap { $0.rx.progress() }
          .observeOn(Self.mainScheduler)
          .do(onCompleted: { isCompleted = true })
          .takeUntil(self.cancelCommand(for: request.identifier))
          .throttle(self.options.throttleProgress, latest: true, scheduler: self.throttleScheduler)
          .distinctUntilChanged()
          .materialize()
          .observeOn(Self.mainScheduler)
          .map { (state: $0.asDownloadState(isCompleted), error: $0.error) }
          .do(
            onNext: { (state, error) in
              if case .completed = state {
                downloadResultS.accept(.success(request))
              }
              else if case .cancelled = state {
                downloadResultS.accept(.cancelled(request))
              }
              else if case .failed = state, let error = error {
                downloadResultS.accept(.failure(request, .downloadError(error)))
              }
            }
          )
          .concatMap { (state, _) -> Completable in
            dataSource.update(
              id: request.identifier,
              state: state
            )
          }
          .asCompletable()
      }
      .catchError { error in
        print("[PhDownloader] Unhandle error: \(error)")
        return .empty()
      }
      .subscribeOn(Self.concurrentMainScheduler)
  }

  /// Filter command cancel task that has id equals to `identifierNeedCancel`
  private func cancelCommand(for identifierNeedCancel: String) -> Observable<Void> {
    self.commandS.compactMap {
      if case .cancel(let identifier) = $0, identifier == identifierNeedCancel {
        return ()
      }
      return nil
    }
  }

  /// Update local database: Update state of task to cancelled
  private func cancelDownload(_ identifier: String) -> Completable {
    Single
      .deferred { [dataSource] () -> Single<DownloadTaskEntity> in
        // get task and check can cancel
        do {
          guard let task = try dataSource.get(by: identifier) else {
            return .error(PhDownloaderError.notFound(identifier: identifier))
          }

          guard task.canCancel else {
            return .error(PhDownloaderError.cannotCancel(identifier: identifier))
          }

          return .just(task)
        } catch {
          return .error(PhDownloaderError.databaseError(error))
        }
      }
      .subscribeOn(Self.concurrentMainScheduler)
      .flatMapCompletable { [dataSource] in dataSource.update(id: $0.identifier, state: .cancelled) }
  }
}

// MARK: Observe state by identifier
extension RealDownloader {
  func observe(by identifier: String) -> Observable<PhDownloadTask?> {
    Observable
      .deferred { [dataSource] () -> Observable<PhDownloadTask?> in
        do {
          if let task = try dataSource.get(by: identifier) {
            return Observable
              .from(object: task, emitInitialValue: true)
              .map { .init(from: $0) }
              .subscribeOn(Self.concurrentMainScheduler)
          }

          return Observable
            .collection(
              from: try dataSource.getResults(by: identifier),
              synchronousStart: true,
              on: .main
            )
            .map { results in results.first.map { .init(from: $0) } }
            .subscribeOn(Self.concurrentMainScheduler)

        } catch {
          return .error(PhDownloaderError.databaseError(error))
        }
      }
      .distinctUntilChanged()
  }

}

// MARK: Observe state by identifiers
extension RealDownloader {
  func observe<T: Sequence>(by identifiers: T) -> Observable<[String: PhDownloadTask]>
  where T.Element == String
  {
      .deferred { [dataSource] () -> Observable<[String: PhDownloadTask]> in
        do {
          return Observable
            .collection(
              from: try dataSource.getResults(by: Set(identifiers)),
              synchronousStart: true,
              on: .main
            )
            .subscribeOn(Self.concurrentMainScheduler)
            .map { results in
              let taskById = results.map { ($0.identifier, PhDownloadTask.init(from: $0)) }
              return Dictionary(uniqueKeysWithValues: taskById)
            }
            .distinctUntilChanged()
        } catch {
          return .error(PhDownloaderError.databaseError(error))
        }
    }
  }
}

// MARK: Download result observable
extension RealDownloader {
  var downloadResult$: Observable<PhDownloadResult> { self.downloadResultS.asObservable() }
}

// MARK: Enqueue download request
extension RealDownloader {
  func enqueue(_ request: PhDownloadRequest) -> Completable {
    // insert or update task into database
    // and then, send command to enqueue download request
    self.dataSource
      .insertOrUpdate(
        identifier: request.identifier,
        url: request.url,
        fileName: request.fileName,
        savedDir: request.savedDir,
        state: .enqueued
      )
      .observeOn(Self.mainScheduler)
      .do(onCompleted: { [commandS] in commandS.accept(.enqueue(request: request)) })
  }
}

// MARK: Cancel by identifier
extension RealDownloader {
  func cancel(by identifier: String) -> Completable {
    // mask task as cancelled to prevent executing enqueued task
    // and then, send command to cancel downloading task
    self.cancelDownload(identifier)
      .observeOn(Self.mainScheduler)
      .do(onCompleted: { [commandS] in commandS.accept(.cancel(identifier: identifier)) })
  }
}

// MARK: Cancel all
extension RealDownloader {
  func cancelAll() -> Completable {
    self.dataSource
      .cancelAll()
      .observeOn(Self.mainScheduler)
      .do(onSuccess: { [commandS] ids in
        ids
          .map { Command.cancel(identifier: $0) }
          .forEach(commandS.accept)
      })
      .asCompletable()
  }
}
